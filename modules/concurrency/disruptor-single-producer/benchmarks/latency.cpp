#include <lls/concurrency/disruptor_single_producer.hpp>

#include <algorithm>
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
#include <thread>
#include <vector>

namespace {

constexpr std::size_t warmup_count = 10'000;
constexpr std::size_t sample_count = 200'000;

using clock_type = std::chrono::steady_clock;

struct event final {
    clock_type::time_point published_at{};
};

struct distribution final {
    double p50_ns{};
    double p99_ns{};
};

void pin_to_cpu(std::size_t cpu_index) noexcept {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu_index, &set);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

[[nodiscard]] distribution summarize(std::vector<std::int64_t> samples) {
    std::sort(samples.begin(), samples.end());
    const auto percentile = [&samples](double fraction) {
        const auto index = static_cast<std::size_t>(
            static_cast<double>(samples.size() - 1) * fraction);
        return static_cast<double>(samples[index]);
    };
    return {percentile(0.50), percentile(0.99)};
}

[[nodiscard]] distribution disruptor_latency() {
    using stream_type = lls::concurrency::single_producer_disruptor<
        event,
        1024,
        1>;
    stream_type stream;
    std::vector<std::int64_t> samples(sample_count);
    std::atomic<bool> start{false};

    std::thread consumer([&] {
        pin_to_cpu(1);
        while (!start.load(std::memory_order_acquire)) {
        }
        std::size_t consumed = 0;
        const auto total = warmup_count + sample_count;
        while (consumed < total) {
            if (stream.try_consume(
                    0,
                    [&](const event& input) noexcept {
                        const auto elapsed =
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                clock_type::now() - input.published_at)
                                .count();
                        if (consumed >= warmup_count) {
                            samples[consumed - warmup_count] = elapsed;
                        }
                    })) {
                ++consumed;
            }
        }
    });

    pin_to_cpu(0);
    start.store(true, std::memory_order_release);
    const auto total = warmup_count + sample_count;
    for (std::size_t sequence = 0; sequence < total; ++sequence) {
        while (!stream.try_publish([](event& output) noexcept {
            output.published_at = clock_type::now();
        })) {
        }
        while (stream.consumer_sequence(0) <
               static_cast<std::int64_t>(sequence)) {
        }
    }
    consumer.join();
    return summarize(std::move(samples));
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
    return summarize(std::move(samples));
}

void print(const char* name, const distribution& result) {
    std::cout << std::left << std::setw(24) << name << std::right << "p50 "
              << std::setw(8) << std::fixed << std::setprecision(0)
              << result.p50_ns << " ns  p99 " << std::setw(8)
              << result.p99_ns << " ns\n";
}

}  // namespace

int main() {
    const auto blocking = blocking_latency();
    const auto disruptor = disruptor_latency();
    print("blocking single slot", blocking);
    print("busy-spin disruptor", disruptor);

    const bool relative_target = disruptor.p50_ns <= blocking.p50_ns * 0.50 &&
                                 disruptor.p99_ns <= blocking.p99_ns * 0.50;
    const bool stretch_target =
        disruptor.p50_ns < 250.0 && disruptor.p99_ns < 1'000.0;
    std::cout << "\nAcceptance\n"
              << "p50/p99 <=50% blocking: "
              << (relative_target ? "PASS" : "FAIL") << '\n'
              << "p50 <250ns, p99 <1us:  "
              << (stretch_target ? "PASS" : "FAIL") << '\n';
    return relative_target && stretch_target ? 0 : 1;
}
