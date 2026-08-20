#include <lls/concurrency/disruptor_single_producer.hpp>
#include <lls/concurrency/spin_wait.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <pthread.h>
#include <sched.h>
#include <string_view>
#include <thread>
#include <vector>

namespace {

constexpr std::uint64_t event_count = 5'000'000;
constexpr std::size_t warmup_count = 10'000;
constexpr std::size_t sample_count = 200'000;
using clock_type = std::chrono::steady_clock;

struct event final {
    std::uint64_t value{};
    clock_type::time_point published_at{};
};

struct empty_spin_wait final {
    void wait() const noexcept {}
    void reset() const noexcept {}
};

struct result final {
    std::string_view name;
    double events_per_second{};
    double p50_ns{};
    double p99_ns{};
    std::uint64_t checksum{};
};

void pin_to_cpu(std::size_t cpu_index) noexcept {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu_index, &set);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

template <typename WaitPolicy>
[[nodiscard]] result measure_throughput(std::string_view name) {
    using stream_type = lls::concurrency::single_producer_disruptor<
        event,
        65'536,
        1>;
    stream_type stream;
    std::atomic<bool> start{false};
    std::uint64_t checksum = 0;

    std::thread consumer([&] {
        WaitPolicy wait;
        pin_to_cpu(1);
        while (!start.load(std::memory_order_acquire)) {
            wait.wait();
        }
        wait.reset();
        std::uint64_t consumed = 0;
        while (consumed < event_count) {
            if (stream.try_consume(
                    0,
                    [&checksum](const event& input) noexcept {
                        checksum += input.value;
                    })) {
                ++consumed;
                wait.reset();
            } else {
                wait.wait();
            }
        }
    });

    WaitPolicy wait;
    pin_to_cpu(0);
    const auto begin = clock_type::now();
    start.store(true, std::memory_order_release);
    for (std::uint64_t value = 0; value < event_count;) {
        if (stream.try_publish([value](event& output) noexcept {
                output.value = value;
            })) {
            ++value;
            wait.reset();
        } else {
            wait.wait();
        }
    }
    consumer.join();
    const auto elapsed =
        std::chrono::duration<double>(clock_type::now() - begin).count();
    return {name,
            static_cast<double>(event_count) / elapsed,
            0.0,
            0.0,
            checksum};
}

template <typename WaitPolicy>
[[nodiscard]] result measure_latency(std::string_view name) {
    using stream_type = lls::concurrency::single_producer_disruptor<
        event,
        1024,
        1>;
    stream_type stream;
    std::vector<std::int64_t> samples(sample_count);
    std::atomic<bool> start{false};

    std::thread consumer([&] {
        WaitPolicy wait;
        pin_to_cpu(1);
        while (!start.load(std::memory_order_acquire)) {
            wait.wait();
        }
        wait.reset();
        std::size_t consumed = 0;
        const auto total = warmup_count + sample_count;
        while (consumed < total) {
            if (stream.try_consume(
                    0,
                    [&](const event& input) noexcept {
                        if (consumed >= warmup_count) {
                            samples[consumed - warmup_count] =
                                std::chrono::duration_cast<
                                    std::chrono::nanoseconds>(
                                    clock_type::now() - input.published_at)
                                    .count();
                        }
                    })) {
                ++consumed;
                wait.reset();
            } else {
                wait.wait();
            }
        }
    });

    WaitPolicy wait;
    pin_to_cpu(0);
    start.store(true, std::memory_order_release);
    const auto total = warmup_count + sample_count;
    for (std::size_t sequence = 0; sequence < total; ++sequence) {
        while (!stream.try_publish([](event& output) noexcept {
            output.published_at = clock_type::now();
        })) {
            wait.wait();
        }
        wait.reset();
        while (stream.consumer_sequence(0) <
               static_cast<decltype(stream.consumer_sequence(0))>(sequence)) {
            wait.wait();
        }
        wait.reset();
    }
    consumer.join();

    std::sort(samples.begin(), samples.end());
    const auto percentile = [&samples](double fraction) {
        const auto index = static_cast<std::size_t>(
            static_cast<double>(samples.size() - 1) * fraction);
        return static_cast<double>(samples[index]);
    };
    return {name, 0.0, percentile(0.50), percentile(0.99), 0};
}

void print_throughput(const result& value) {
    std::cout << std::left << std::setw(18) << value.name << std::right
              << std::fixed << std::setprecision(2)
              << value.events_per_second / 1'000'000.0 << " M events/s\n";
}

void print_latency(const result& value) {
    std::cout << std::left << std::setw(18) << value.name << std::right
              << "p50 " << std::setw(6) << std::fixed << std::setprecision(0)
              << value.p50_ns << " ns  p99 " << std::setw(6) << value.p99_ns
              << " ns\n";
}

}  // namespace

int main() {
    const auto empty_throughput =
        measure_throughput<empty_spin_wait>("empty spin");
    const auto pause_throughput =
        measure_throughput<lls::concurrency::busy_spin_wait>("PAUSE");
    const auto adaptive_throughput = measure_throughput<
        lls::concurrency::adaptive_spin_wait<64>>("adaptive 64");

    const auto empty_latency = measure_latency<empty_spin_wait>("empty spin");
    const auto pause_latency =
        measure_latency<lls::concurrency::busy_spin_wait>("PAUSE");
    const auto adaptive_latency =
        measure_latency<lls::concurrency::adaptive_spin_wait<64>>(
            "adaptive 64");

    std::cout << "Throughput\n";
    print_throughput(empty_throughput);
    print_throughput(pause_throughput);
    print_throughput(adaptive_throughput);
    std::cout << "\nHandoff latency\n";
    print_latency(empty_latency);
    print_latency(pause_latency);
    print_latency(adaptive_latency);

    const auto expected = (event_count - 1) * event_count / 2;
    const bool correct = empty_throughput.checksum == expected &&
                         pause_throughput.checksum == expected &&
                         adaptive_throughput.checksum == expected;
    std::cout << "\ncorrect checksums: " << (correct ? "PASS" : "FAIL")
              << '\n';
    return correct ? 0 : 1;
}
