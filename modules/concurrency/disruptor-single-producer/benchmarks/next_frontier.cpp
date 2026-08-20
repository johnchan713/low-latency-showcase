#include <lls/concurrency/disruptor_single_producer.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <pthread.h>
#include <sched.h>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using clock_type = std::chrono::steady_clock;
constexpr std::uint64_t event_count = 20'000'000;
constexpr std::size_t capacity = 65'536;

struct event final {
    std::uint64_t value{};
};

struct timed_event final {
    clock_type::time_point published_at{};
};

void pin_to_cpu(std::size_t cpu) noexcept {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    const auto result = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (result != 0) {
        std::cerr << "failed to pin benchmark thread to CPU " << cpu
                  << " (error " << result << ")\n";
        std::abort();
    }
}

template <std::size_t ConsumerIndex,
          bool UseHandle,
          std::size_t DrainBatch,
          typename Stream,
          std::size_t ConsumerCount>
void consume(Stream& stream,
             std::atomic<bool>& start,
             std::atomic<std::size_t>& ready,
             std::array<std::uint64_t, ConsumerCount>& checksums) {
    pin_to_cpu(ConsumerIndex + 1);
    auto handle = stream.template make_consumer<ConsumerIndex>();
    ready.fetch_add(1, std::memory_order_release);
    while (!start.load(std::memory_order_acquire)) {
    }

    std::uint64_t consumed = 0;
    std::uint64_t expected = 0;
    std::uint64_t checksum = 0;
    bool ordered = true;
    const auto handler = [&](const event& input,
                             std::uint64_t sequence) noexcept {
        ordered = ordered && sequence == expected && input.value == expected;
        checksum += input.value;
        ++expected;
    };
    while (consumed < event_count) {
        if constexpr (UseHandle) {
            consumed += handle.consume_available(DrainBatch, handler);
        } else {
            consumed += stream.consume_available(
                ConsumerIndex, DrainBatch, handler);
        }
    }
    checksums[ConsumerIndex] =
        ordered ? checksum : std::numeric_limits<std::uint64_t>::max();
}

template <std::size_t PublishBatch,
          std::size_t DrainBatch,
          bool UseHandle,
          std::size_t ConsumerCount>
void throughput(std::string_view name) {
    using stream_type = lls::concurrency::single_producer_disruptor<
        event,
        capacity,
        ConsumerCount>;
    stream_type stream;
    std::atomic<bool> start{false};
    std::atomic<std::size_t> ready{0};
    std::array<std::uint64_t, ConsumerCount> checksums{};
    std::array<std::thread, ConsumerCount> threads;

    [&]<std::size_t... Index>(std::index_sequence<Index...>) {
        ((threads[Index] = std::thread([&] {
              consume<Index, UseHandle, DrainBatch>(
                  stream, start, ready, checksums);
          })),
         ...);
    }(std::make_index_sequence<ConsumerCount>{});

    pin_to_cpu(0);
    while (ready.load(std::memory_order_acquire) != ConsumerCount) {
    }
    const auto begin = clock_type::now();
    start.store(true, std::memory_order_release);
    std::uint64_t published = 0;
    while (published < event_count) {
        const auto count = std::min<std::size_t>(
            PublishBatch,
            static_cast<std::size_t>(event_count - published));
        if (stream.try_publish_batch(
                count,
                [published](event& output, std::size_t index) noexcept {
                    output.value = published + index;
                })) {
            published += count;
        }
    }
    for (auto& thread : threads) {
        thread.join();
    }
    const auto seconds =
        std::chrono::duration<double>(clock_type::now() - begin).count();
    const auto expected_checksum = (event_count - 1) * event_count / 2;
    const auto correct = std::all_of(
        checksums.begin(),
        checksums.end(),
        [expected_checksum](std::uint64_t checksum) {
            return checksum == expected_checksum;
        });
    std::cout << "throughput," << name << ',' << PublishBatch << ','
              << DrainBatch << ',' << ConsumerCount << ',' << std::fixed
              << std::setprecision(2)
              << static_cast<double>(event_count) / seconds / 1'000'000.0
              << ",0," << (correct ? "PASS" : "FAIL") << '\n';
}

template <std::size_t ConsumerCount>
void full_retry(std::string_view name) {
    constexpr std::size_t retry_capacity = 1024;
    constexpr std::uint64_t attempts = 100'000'000;
    using stream_type = lls::concurrency::single_producer_disruptor<
        event,
        retry_capacity,
        ConsumerCount>;
    stream_type stream;
    if (!stream.try_publish_batch(
            retry_capacity,
            [](event& output, std::size_t index) noexcept {
                output.value = index;
            })) {
        std::abort();
    }
    for (std::size_t consumer = 0; consumer + 1 < ConsumerCount; ++consumer) {
        const auto consumed = stream.consume_available(
            consumer,
            retry_capacity,
            [](const event&, std::uint64_t) noexcept {});
        if (consumed != retry_capacity) {
            std::abort();
        }
    }

    const auto writer = [](event&) noexcept {};
    if (stream.try_publish(writer)) {
        std::abort();
    }
    const auto begin = clock_type::now();
    std::uint64_t rejected = 0;
    for (std::uint64_t attempt = 0; attempt < attempts; ++attempt) {
        rejected += stream.try_publish(writer) ? 0U : 1U;
    }
    const auto seconds =
        std::chrono::duration<double>(clock_type::now() - begin).count();
    std::cout << "full-retry," << name << ",1,0," << ConsumerCount << ','
              << std::fixed << std::setprecision(2)
              << static_cast<double>(attempts) / seconds / 1'000'000.0 << ",0,"
              << (rejected == attempts ? "PASS" : "FAIL") << '\n';
}

template <std::size_t BatchSize, bool UseHandle>
void latency(std::string_view name) {
    constexpr std::size_t warmup = 5'000;
    constexpr std::size_t samples_count = 100'000;
    constexpr std::size_t total = warmup + samples_count;
    using stream_type = lls::concurrency::single_producer_disruptor<
        timed_event,
        1024,
        1>;
    stream_type stream;
    std::atomic<bool> start{false};
    std::atomic<bool> ready{false};
    std::vector<std::int64_t> samples(samples_count);
    bool ordered = true;

    std::thread thread([&] {
        pin_to_cpu(1);
        auto handle = stream.template make_consumer<0>();
        ready.store(true, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
        }
        std::size_t consumed = 0;
        const auto handler = [&](const timed_event& input,
                                 std::uint64_t sequence) noexcept {
            ordered = ordered && sequence == consumed;
            if (sequence >= warmup) {
                samples[sequence - warmup] =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        clock_type::now() - input.published_at)
                        .count();
            }
            ++consumed;
        };
        while (consumed < total) {
            if constexpr (UseHandle) {
                (void)handle.consume_available(BatchSize, handler);
            } else {
                (void)stream.consume_available(0, BatchSize, handler);
            }
        }
    });

    pin_to_cpu(0);
    while (!ready.load(std::memory_order_acquire)) {
    }
    start.store(true, std::memory_order_release);
    for (std::size_t published = 0; published < total;) {
        const auto count = std::min(BatchSize, total - published);
        while (!stream.try_publish_batch(
            count,
            [](timed_event& output, std::size_t) noexcept {
                output.published_at = clock_type::now();
            })) {
        }
        published += count;
        while (stream.consumer_position(0) != published) {
        }
    }
    thread.join();
    std::sort(samples.begin(), samples.end());
    const auto percentile = [&samples](double fraction) {
        return samples[static_cast<std::size_t>(
            static_cast<double>(samples.size() - 1) * fraction)];
    };
    std::cout << "latency," << name << ',' << BatchSize << ',' << BatchSize
              << ",1," << percentile(0.50) << ',' << percentile(0.99) << ','
              << (ordered ? "PASS" : "FAIL") << '\n';
}

}  // namespace

int main() {
    pin_to_cpu(0);
    std::cout << "kind,name,publish_batch,drain_batch,consumers,value1,value2,"
                 "correct\n";
    throughput<1, 1, false, 1>("indexed");
    throughput<1, 1, true, 1>("handle");
    throughput<1, 64, false, 1>("indexed");
    throughput<1, 64, true, 1>("handle");
    throughput<16, 16, false, 1>("indexed");
    throughput<16, 16, true, 1>("handle");
    throughput<16, 64, false, 1>("indexed");
    throughput<16, 64, true, 1>("handle");
    throughput<64, 8, false, 1>("indexed");
    throughput<64, 8, true, 1>("handle");
    throughput<64, 64, false, 1>("indexed");
    throughput<64, 64, true, 1>("handle");
    throughput<64, 256, false, 1>("indexed");
    throughput<64, 256, true, 1>("handle");
    throughput<16, 16, false, 3>("indexed");
    throughput<16, 16, true, 3>("handle");
    throughput<16, 64, false, 3>("indexed");
    throughput<16, 64, true, 3>("handle");
    latency<1, false>("indexed");
    latency<1, true>("handle");
    latency<16, false>("indexed");
    latency<16, true>("handle");
    full_retry<3>("cached-blocker");
    full_retry<8>("cached-blocker");
}
