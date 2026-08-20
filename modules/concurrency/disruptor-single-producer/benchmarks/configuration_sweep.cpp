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
#include <vector>

namespace {

using clock_type = std::chrono::steady_clock;
constexpr std::uint64_t event_count = 5'000'000;
constexpr std::size_t latency_warmup = 5'000;
constexpr std::size_t latency_samples = 50'000;

struct event_8 final {
    std::uint64_t value{};
};

struct event_64 final {
    std::uint64_t value{};
    std::array<std::uint64_t, 7> payload{};
};

struct timed_event_8 final {
    clock_type::time_point published_at{};
};

struct timed_event_64 final {
    clock_type::time_point published_at{};
    std::array<std::uint64_t, 7> payload{};
};

static_assert(sizeof(event_8) == 8);
static_assert(sizeof(event_64) == 64);
static_assert(sizeof(timed_event_8) == 8);
static_assert(sizeof(timed_event_64) == 64);

template <typename Event>
void fill_payload(Event& output, std::uint64_t sequence) noexcept {
    if constexpr (requires { output.payload; }) {
        for (std::size_t index = 0; index < output.payload.size(); ++index) {
            output.payload[index] = sequence + index + 1;
        }
    }
}

template <typename Event>
[[nodiscard]] bool payload_is_correct(const Event& input,
                                      std::uint64_t sequence) noexcept {
    if constexpr (requires { input.payload; }) {
        for (std::size_t index = 0; index < input.payload.size(); ++index) {
            if (input.payload[index] != sequence + index + 1) {
                return false;
            }
        }
    }
    return true;
}

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

template <std::size_t Capacity,
          std::size_t BatchSize,
          typename Event,
          std::size_t ConsumerCount>
void throughput(std::string_view name) {
    using stream_type = lls::concurrency::single_producer_disruptor<
        Event,
        Capacity,
        ConsumerCount>;
    stream_type stream;
    std::atomic<bool> start{false};
    std::atomic<std::size_t> ready{0};
    std::array<std::uint64_t, ConsumerCount> checksums{};
    std::array<std::thread, ConsumerCount> consumers;

    for (std::size_t consumer_index = 0; consumer_index < ConsumerCount;
         ++consumer_index) {
        consumers[consumer_index] = std::thread([&, consumer_index] {
            pin_to_cpu(consumer_index + 1);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
            }
            std::uint64_t consumed = 0;
            std::uint64_t checksum = 0;
            std::uint64_t expected_sequence = 0;
            bool ordered = true;
            while (consumed < event_count) {
                consumed += stream.consume_available(
                    consumer_index,
                    BatchSize,
                    [&](const Event& input, std::uint64_t sequence) noexcept {
                        ordered = ordered && sequence == expected_sequence &&
                                  input.value == expected_sequence &&
                                  payload_is_correct(input, expected_sequence);
                        checksum += input.value;
                        ++expected_sequence;
                    });
            }
            checksums[consumer_index] =
                ordered ? checksum : std::numeric_limits<std::uint64_t>::max();
        });
    }

    pin_to_cpu(0);
    while (ready.load(std::memory_order_acquire) != ConsumerCount) {
    }
    const auto begin = clock_type::now();
    start.store(true, std::memory_order_release);
    std::uint64_t published = 0;
    while (published < event_count) {
        const auto count = std::min<std::size_t>(
            BatchSize, static_cast<std::size_t>(event_count - published));
        if (stream.try_publish_batch(
                count,
                [published](Event& output, std::size_t index) noexcept {
                    output.value = published + index;
                    fill_payload(output, published + index);
                })) {
            published += count;
        }
    }
    for (auto& consumer : consumers) {
        consumer.join();
    }
    const auto seconds =
        std::chrono::duration<double>(clock_type::now() - begin).count();
    const auto expected = (event_count - 1) * event_count / 2;
    const auto correct = std::all_of(
        checksums.begin(), checksums.end(), [expected](std::uint64_t value) {
            return value == expected;
        });
    std::cout << "T," << name << ',' << Capacity << ',' << BatchSize << ','
              << sizeof(Event) << ',' << ConsumerCount << ',' << std::fixed
              << std::setprecision(2)
              << static_cast<double>(event_count) / seconds / 1'000'000.0
              << ',' << (correct ? "PASS" : "FAIL") << '\n';
}

template <std::size_t BatchSize,
          typename Event,
          std::size_t ConsumerCount>
void latency(std::string_view name) {
    constexpr std::size_t capacity = 1024;
    constexpr std::size_t total = latency_warmup + latency_samples;
    using stream_type = lls::concurrency::single_producer_disruptor<
        Event,
        capacity,
        ConsumerCount>;
    stream_type stream;
    std::atomic<bool> start{false};
    std::atomic<std::size_t> ready{0};
    std::vector<std::int64_t> samples(latency_samples * ConsumerCount);
    std::array<bool, ConsumerCount> correct{};
    std::array<std::thread, ConsumerCount> consumers;

    for (std::size_t consumer_index = 0; consumer_index < ConsumerCount;
         ++consumer_index) {
        consumers[consumer_index] = std::thread([&, consumer_index] {
            pin_to_cpu(consumer_index + 1);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
            }
            std::size_t consumed = 0;
            std::uint64_t expected_sequence = 0;
            bool ordered = true;
            while (consumed < total) {
                consumed += stream.consume_available(
                    consumer_index,
                    BatchSize,
                    [&, consumer_index](const Event& input,
                                        std::uint64_t sequence) noexcept {
                        if (sequence >= latency_warmup) {
                            const auto index =
                                static_cast<std::size_t>(sequence) -
                                latency_warmup;
                            samples[consumer_index * latency_samples + index] =
                                std::chrono::duration_cast<
                                    std::chrono::nanoseconds>(
                                    clock_type::now() - input.published_at)
                                    .count();
                        }
                        ordered = ordered && sequence == expected_sequence &&
                                  payload_is_correct(input, sequence);
                        ++expected_sequence;
                    });
            }
            correct[consumer_index] = ordered;
        });
    }

    pin_to_cpu(0);
    while (ready.load(std::memory_order_acquire) != ConsumerCount) {
    }
    start.store(true, std::memory_order_release);
    for (std::size_t published = 0; published < total;) {
        const auto count = std::min(BatchSize, total - published);
        while (!stream.try_publish_batch(
            count,
            [published](Event& output, std::size_t index) noexcept {
                output.published_at = clock_type::now();
                fill_payload(output, published + index);
            })) {
        }
        published += count;
        const auto position = static_cast<std::uint64_t>(published);
        for (std::size_t consumer_index = 0;
             consumer_index < ConsumerCount;
             ++consumer_index) {
            while (stream.consumer_position(consumer_index) != position) {
            }
        }
    }
    for (auto& consumer : consumers) {
        consumer.join();
    }
    if (!std::all_of(correct.begin(), correct.end(), [](bool value) {
            return value;
        })) {
        std::cerr << "latency payload or ordering validation failed\n";
        std::abort();
    }
    std::sort(samples.begin(), samples.end());
    const auto percentile = [&samples](double fraction) {
        return samples[static_cast<std::size_t>(
            static_cast<double>(samples.size() - 1) * fraction)];
    };
    std::cout << "L," << name << ",1024," << BatchSize << ','
              << sizeof(Event) << ',' << ConsumerCount << ','
              << percentile(0.50) << ',' << percentile(0.99) << '\n';
}

template <std::size_t Capacity, typename Event>
void throughput_batches(std::string_view name) {
    throughput<Capacity, 1, Event, 1>(name);
    throughput<Capacity, 2, Event, 1>(name);
    throughput<Capacity, 4, Event, 1>(name);
    throughput<Capacity, 8, Event, 1>(name);
    throughput<Capacity, 16, Event, 1>(name);
    throughput<Capacity, 32, Event, 1>(name);
    throughput<Capacity, 64, Event, 1>(name);
    throughput<Capacity, 128, Event, 1>(name);
}

template <typename Event>
void latency_batches(std::string_view name) {
    latency<1, Event, 1>(name);
    latency<2, Event, 1>(name);
    latency<4, Event, 1>(name);
    latency<8, Event, 1>(name);
    latency<16, Event, 1>(name);
    latency<32, Event, 1>(name);
    latency<64, Event, 1>(name);
    latency<128, Event, 1>(name);
}

}  // namespace

int main() {
    pin_to_cpu(0);
    std::cout << "kind,name,capacity,batch,payload,consumers,value1,value2\n";
    throughput_batches<1024, event_8>("event8");
    throughput_batches<65'536, event_8>("event8");
    throughput_batches<1'048'576, event_8>("event8");
    throughput_batches<1024, event_64>("event64");
    throughput_batches<65'536, event_64>("event64");
    throughput_batches<1'048'576, event_64>("event64");
    throughput<65'536, 1, event_8, 3>("multicast8");
    throughput<65'536, 16, event_8, 3>("multicast8");
    throughput<65'536, 64, event_8, 3>("multicast8");
    latency_batches<timed_event_8>("event8");
    latency_batches<timed_event_64>("event64");
    latency<1, timed_event_8, 3>("multicast8");
    latency<16, timed_event_8, 3>("multicast8");
    latency<64, timed_event_8, 3>("multicast8");
}
