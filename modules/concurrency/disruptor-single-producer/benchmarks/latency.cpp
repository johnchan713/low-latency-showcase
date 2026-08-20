#include <lls/concurrency/disruptor_single_producer.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <sched.h>
#include <string_view>
#include <thread>
#include <vector>

namespace {

constexpr std::size_t warmup_count = 10'000;
constexpr std::size_t sample_count = 200'000;
using clock_type = std::chrono::steady_clock;

struct event_8 final {
    clock_type::time_point published_at{};
};

struct event_64 final {
    clock_type::time_point published_at{};
    std::array<std::byte, 56> payload{};
};

static_assert(sizeof(event_8) == 8);
static_assert(sizeof(event_64) == 64);

struct distribution final {
    std::string_view name;
    double p50_ns{};
    double p99_ns{};
    bool correct{};
};

void pin_to_cpu(std::size_t cpu_index) noexcept {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu_index, &set);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

[[nodiscard]] distribution summarize(std::string_view name,
                                     std::vector<std::int64_t> samples,
                                     bool correct) {
    std::sort(samples.begin(), samples.end());
    const auto percentile = [&samples](double fraction) {
        const auto index = static_cast<std::size_t>(
            static_cast<double>(samples.size() - 1) * fraction);
        return static_cast<double>(samples[index]);
    };
    return {name, percentile(0.50), percentile(0.99), correct};
}

template <typename Event, std::size_t BatchSize, std::size_t ConsumerCount>
[[nodiscard]] distribution disruptor_latency(std::string_view name) {
    using stream_type = lls::concurrency::single_producer_disruptor<
        Event,
        1024,
        ConsumerCount>;
    stream_type stream;
    std::vector<std::int64_t> samples(sample_count * ConsumerCount);
    std::array<std::uint64_t, ConsumerCount> checksums{};
    std::array<std::thread, ConsumerCount> consumers;
    std::atomic<bool> start{false};
    constexpr auto total = warmup_count + sample_count;

    for (std::size_t consumer_index = 0; consumer_index < ConsumerCount;
         ++consumer_index) {
        consumers[consumer_index] = std::thread([&, consumer_index] {
            pin_to_cpu(consumer_index + 1);
            while (!start.load(std::memory_order_acquire)) {
            }
            std::size_t consumed = 0;
            while (consumed < total) {
                consumed += stream.consume_available(
                    consumer_index,
                    BatchSize,
                    [&](const Event& input, std::int64_t sequence) noexcept {
                        const auto elapsed =
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                clock_type::now() - input.published_at)
                                .count();
                        if (sequence >=
                            static_cast<std::int64_t>(warmup_count)) {
                            const auto sample_index =
                                static_cast<std::size_t>(sequence) -
                                warmup_count;
                            samples[consumer_index * sample_count +
                                    sample_index] = elapsed;
                        }
                        checksums[consumer_index] +=
                            static_cast<std::uint64_t>(sequence);
                    });
            }
        });
    }

    pin_to_cpu(0);
    start.store(true, std::memory_order_release);
    for (std::size_t published = 0; published < total;) {
        const auto count = std::min(BatchSize, total - published);
        while (!stream.try_publish_batch(
            count,
            [](Event& output, std::size_t) noexcept {
                output.published_at = clock_type::now();
            })) {
        }
        published += count;
        const auto last_sequence = static_cast<std::int64_t>(published - 1);
        for (std::size_t consumer_index = 0;
             consumer_index < ConsumerCount;
             ++consumer_index) {
            while (stream.consumer_sequence(consumer_index) < last_sequence) {
            }
        }
    }
    for (auto& consumer : consumers) {
        consumer.join();
    }

    const auto expected = static_cast<std::uint64_t>(total - 1) * total / 2;
    const bool correct = std::all_of(
        checksums.begin(), checksums.end(), [expected](std::uint64_t checksum) {
            return checksum == expected;
        });
    return summarize(name, std::move(samples), correct);
}

class blocking_slot final {
public:
    void publish(clock_type::time_point published_at) {
        std::unique_lock lock{mutex_};
        consumed_.wait(lock, [this] { return !full_; });
        published_at_ = published_at;
        full_ = true;
        available_.notify_one();
    }

    [[nodiscard]] clock_type::time_point consume() {
        std::unique_lock lock{mutex_};
        available_.wait(lock, [this] { return full_; });
        const auto published_at = published_at_;
        full_ = false;
        consumed_.notify_one();
        return published_at;
    }

private:
    std::mutex mutex_;
    std::condition_variable available_;
    std::condition_variable consumed_;
    clock_type::time_point published_at_{};
    bool full_{};
};

[[nodiscard]] distribution blocking_latency() {
    blocking_slot slot;
    std::vector<std::int64_t> samples(sample_count);
    std::atomic<bool> start{false};

    std::thread consumer([&] {
        pin_to_cpu(1);
        while (!start.load(std::memory_order_acquire)) {
        }
        const auto total = warmup_count + sample_count;
        for (std::size_t consumed = 0; consumed < total; ++consumed) {
            const auto published_at = slot.consume();
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    clock_type::now() - published_at)
                    .count();
            if (consumed >= warmup_count) {
                samples[consumed - warmup_count] = elapsed;
            }
        }
    });

    pin_to_cpu(0);
    start.store(true, std::memory_order_release);
    const auto total = warmup_count + sample_count;
    for (std::size_t sequence = 0; sequence < total; ++sequence) {
        slot.publish(clock_type::now());
    }
    consumer.join();
    return summarize("blocking single slot", std::move(samples), true);
}

void print(const distribution& result) {
    std::cout << std::left << std::setw(30) << result.name << std::right
              << "p50 " << std::setw(8) << std::fixed << std::setprecision(0)
              << result.p50_ns << " ns  p99 " << std::setw(8)
              << result.p99_ns << " ns  checksum "
              << (result.correct ? "PASS" : "FAIL") << '\n';
}

}  // namespace

int main() {
    const auto blocking = blocking_latency();
    const auto event_8_batch_1 =
        disruptor_latency<event_8, 1, 1>("8-byte batch=1");
    const auto event_8_batch_16 =
        disruptor_latency<event_8, 16, 1>("8-byte batch=16");
    const auto event_64_batch_1 =
        disruptor_latency<event_64, 1, 1>("64-byte batch=1");
    const auto event_64_batch_16 =
        disruptor_latency<event_64, 16, 1>("64-byte batch=16");
    const auto multicast =
        disruptor_latency<event_8, 16, 3>("8-byte batch=16 multicast x3");

    for (const auto& result : {blocking,
                               event_8_batch_1,
                               event_8_batch_16,
                               event_64_batch_1,
                               event_64_batch_16,
                               multicast}) {
        print(result);
    }

    const bool correct = event_8_batch_1.correct && event_8_batch_16.correct &&
                         event_64_batch_1.correct &&
                         event_64_batch_16.correct && multicast.correct;
    const bool relative_target =
        event_8_batch_1.p50_ns <= blocking.p50_ns * 0.50 &&
        event_8_batch_1.p99_ns <= blocking.p99_ns * 0.50;
    const bool stretch_target = event_8_batch_1.p50_ns < 250.0 &&
                                event_8_batch_1.p99_ns < 1'000.0;
    std::cout << "\nAcceptance\n"
              << "correct checksums:       " << (correct ? "PASS" : "FAIL")
              << '\n'
              << "p50/p99 <=50% blocking: "
              << (relative_target ? "PASS" : "FAIL") << '\n'
              << "p50 <250ns, p99 <1us:   "
              << (stretch_target ? "PASS" : "FAIL") << '\n';
    return correct && relative_target && stretch_target ? 0 : 1;
}
