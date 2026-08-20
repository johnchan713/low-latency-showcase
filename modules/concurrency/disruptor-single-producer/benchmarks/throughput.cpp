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
#include <mutex>
#include <pthread.h>
#include <sched.h>
#include <string_view>
#include <thread>
#include <vector>

namespace {

constexpr std::size_t capacity = 65'536;
constexpr std::uint64_t event_count = 5'000'000;

struct event final {
    std::uint64_t value{};
};

struct event_64 final {
    std::uint64_t value{};
    std::array<std::uint64_t, 7> payload{};
};

static_assert(sizeof(event_64) == 64);

struct measurement final {
    std::string_view name;
    double events_per_second{};
    std::uint64_t checksum{};
};

[[nodiscard]] std::uint64_t expected_checksum() noexcept {
    return (event_count - 1) * event_count / 2;
}

template <typename Event>
void write_event(Event& output, std::uint64_t value) noexcept {
    output.value = value;
    if constexpr (requires { output.payload; }) {
        output.payload.fill(value);
    }
}

template <typename Event>
[[nodiscard]] std::uint64_t event_checksum(const Event& input) noexcept {
    auto checksum = input.value;
    if constexpr (requires { input.payload; }) {
        for (const auto value : input.payload) {
            checksum += value;
        }
    }
    return checksum;
}

void pin_to_cpu(std::size_t cpu_index) noexcept {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu_index, &set);
    const auto result = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (result != 0) {
        std::cerr << "failed to pin benchmark thread to CPU " << cpu_index
                  << " (error " << result << ")\n";
        std::abort();
    }
}

template <typename Producer, typename Consumer>
[[nodiscard]] measurement run_pair(std::string_view name,
                                   Producer&& producer,
                                   Consumer&& consumer) {
    std::atomic<bool> start{false};
    std::atomic<bool> ready{false};
    std::uint64_t checksum = 0;

    std::thread consumer_thread([&] {
        pin_to_cpu(1);
        ready.store(true, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        consumer(checksum);
    });

    pin_to_cpu(0);
    while (!ready.load(std::memory_order_acquire)) {
    }
    const auto begin = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    producer();
    consumer_thread.join();
    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - begin);

    return {name, static_cast<double>(event_count) / elapsed.count(), checksum};
}

template <std::size_t BatchSize, typename Event = event>
[[nodiscard]] measurement benchmark_disruptor() {
    using stream_type = lls::concurrency::single_producer_disruptor<
        Event,
        capacity,
        1>;
    stream_type stream;

    return run_pair(
        sizeof(Event) == 64
            ? (BatchSize == 1 ? "disruptor 64-byte batch=1"
                              : "disruptor 64-byte batch=16")
            : (BatchSize == 1 ? "disruptor batch=1"
                              : "disruptor batch=16"),
        [&] {
            std::uint64_t published = 0;
            while (published < event_count) {
                const auto remaining = event_count - published;
                const auto count = static_cast<std::size_t>(
                    remaining < BatchSize ? remaining : BatchSize);
                if (stream.try_publish_batch(
                        count,
                        [published](Event& output,
                                    std::size_t index) noexcept {
                            write_event(output, published + index);
                    })) {
                    published += count;
                }
            }
        },
        [&](std::uint64_t& checksum) {
            std::uint64_t consumed = 0;
            while (consumed < event_count) {
                consumed += stream.consume_available(
                    0,
                    BatchSize,
                    [&checksum](const Event& input,
                                std::uint64_t) noexcept {
                        checksum += event_checksum(input);
                    });
            }
        });
}

class mutex_ring final {
public:
    [[nodiscard]] bool try_push(std::uint64_t value) {
        std::scoped_lock lock{mutex_};
        if (size_ == capacity) {
            return false;
        }
        values_[write_index_] = value;
        write_index_ = (write_index_ + 1) % capacity;
        ++size_;
        return true;
    }

    [[nodiscard]] bool try_pop(std::uint64_t& value) {
        std::scoped_lock lock{mutex_};
        if (size_ == 0) {
            return false;
        }
        value = values_[read_index_];
        read_index_ = (read_index_ + 1) % capacity;
        --size_;
        return true;
    }

private:
    std::array<std::uint64_t, capacity> values_{};
    std::size_t read_index_{};
    std::size_t write_index_{};
    std::size_t size_{};
    std::mutex mutex_;
};

[[nodiscard]] measurement benchmark_mutex_ring() {
    mutex_ring queue;
    return run_pair(
        "mutex ring",
        [&] {
            for (std::uint64_t value = 0; value < event_count;) {
                if (queue.try_push(value)) {
                    ++value;
                }
            }
        },
        [&](std::uint64_t& checksum) {
            std::uint64_t value = 0;
            for (std::uint64_t consumed = 0; consumed < event_count;) {
                if (queue.try_pop(value)) {
                    checksum += value;
                    ++consumed;
                }
            }
        });
}

class spsc_ring final {
public:
    [[nodiscard]] bool try_push(std::uint64_t value) noexcept {
        const auto write = write_.load(std::memory_order_relaxed);
        const auto next = write + 1;
        if (next - read_.load(std::memory_order_acquire) > capacity) {
            return false;
        }
        values_[write & (capacity - 1)] = value;
        write_.store(next, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool try_pop(std::uint64_t& value) noexcept {
        const auto read = read_.load(std::memory_order_relaxed);
        if (read == write_.load(std::memory_order_acquire)) {
            return false;
        }
        value = values_[read & (capacity - 1)];
        read_.store(read + 1, std::memory_order_release);
        return true;
    }

private:
    alignas(64) std::array<std::uint64_t, capacity> values_{};
    alignas(64) std::atomic<std::uint64_t> read_{0};
    alignas(64) std::atomic<std::uint64_t> write_{0};
};

[[nodiscard]] measurement benchmark_spsc_ring() {
    spsc_ring queue;
    return run_pair(
        "specialized SPSC",
        [&] {
            for (std::uint64_t value = 0; value < event_count;) {
                if (queue.try_push(value)) {
                    ++value;
                }
            }
        },
        [&](std::uint64_t& checksum) {
            std::uint64_t value = 0;
            for (std::uint64_t consumed = 0; consumed < event_count;) {
                if (queue.try_pop(value)) {
                    checksum += value;
                    ++consumed;
                }
            }
        });
}

[[nodiscard]] measurement benchmark_multicast_disruptor() {
    constexpr std::size_t consumers = 3;
    using stream_type = lls::concurrency::single_producer_disruptor<
        event,
        capacity,
        consumers>;
    stream_type stream;
    std::atomic<bool> start{false};
    std::atomic<std::size_t> ready{0};
    std::array<std::uint64_t, consumers> checksums{};
    std::array<std::thread, consumers> threads;

    for (std::size_t index = 0; index < consumers; ++index) {
        threads[index] = std::thread([&, index] {
            pin_to_cpu(index + 1);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            std::uint64_t consumed = 0;
            std::uint64_t checksum = 0;
            while (consumed < event_count) {
                const auto handler =
                    [&checksum](const event& input, std::uint64_t) noexcept {
                        checksum += input.value;
                    };
                consumed += stream.consume_available(index, 16, handler);
            }
            checksums[index] = checksum;
        });
    }

    pin_to_cpu(0);
    while (ready.load(std::memory_order_acquire) != consumers) {
    }
    const auto begin = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    std::uint64_t published = 0;
    while (published < event_count) {
        const auto remaining = event_count - published;
        const auto count = static_cast<std::size_t>(remaining < 16 ? remaining : 16);
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
    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - begin);
    return {"disruptor multicast x3",
            static_cast<double>(event_count) / elapsed.count(),
            checksums[0] + checksums[1] + checksums[2]};
}

[[nodiscard]] measurement benchmark_three_spsc_rings() {
    constexpr std::size_t consumers = 3;
    std::array<spsc_ring, consumers> queues;
    std::atomic<bool> start{false};
    std::atomic<std::size_t> ready{0};
    std::array<std::uint64_t, consumers> checksums{};
    std::array<std::thread, consumers> threads;

    for (std::size_t index = 0; index < consumers; ++index) {
        threads[index] = std::thread([&, index] {
            pin_to_cpu(index + 1);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            std::uint64_t value = 0;
            std::uint64_t checksum = 0;
            for (std::uint64_t consumed = 0; consumed < event_count;) {
                if (queues[index].try_pop(value)) {
                    checksum += value;
                    ++consumed;
                }
            }
            checksums[index] = checksum;
        });
    }

    pin_to_cpu(0);
    while (ready.load(std::memory_order_acquire) != consumers) {
    }
    const auto begin = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (std::uint64_t value = 0; value < event_count; ++value) {
        for (auto& queue : queues) {
            while (!queue.try_push(value)) {
            }
        }
    }
    for (auto& thread : threads) {
        thread.join();
    }
    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - begin);
    return {"three SPSC queues",
            static_cast<double>(event_count) / elapsed.count(),
            checksums[0] + checksums[1] + checksums[2]};
}

void print(const measurement& result) {
    std::cout << std::left << std::setw(31) << result.name << std::right
              << std::fixed << std::setprecision(2)
              << result.events_per_second / 1'000'000.0 << " M events/s\n";
}

}  // namespace

int main() {
    pin_to_cpu(0);
    // Run the most contended baseline first so later comparisons do not give it
    // an accidental warm-cache advantage. Repeat externally for serious work.
    const auto mutex = benchmark_mutex_ring();
    const auto spsc = benchmark_spsc_ring();
    const auto batch_one = benchmark_disruptor<1>();
    const auto batch_sixteen = benchmark_disruptor<16>();
    const auto payload_64 = benchmark_disruptor<1, event_64>();
    const auto payload_64_batch = benchmark_disruptor<16, event_64>();
    const auto three_queues = benchmark_three_spsc_rings();
    const auto multicast = benchmark_multicast_disruptor();

    for (const auto& result :
         {mutex,
          spsc,
          batch_one,
          batch_sixteen,
          payload_64,
          payload_64_batch,
          three_queues,
          multicast}) {
        print(result);
    }

    const auto checksum = expected_checksum();
    const bool correct =
        mutex.checksum == checksum && spsc.checksum == checksum &&
        batch_one.checksum == checksum &&
        batch_sixteen.checksum == checksum &&
        payload_64.checksum == checksum * 8 &&
        payload_64_batch.checksum == checksum * 8 &&
        three_queues.checksum == checksum * 3 &&
        multicast.checksum == checksum * 3;
    const bool mutex_target =
        batch_one.events_per_second >= mutex.events_per_second * 2.0;
    const bool spsc_target =
        batch_one.events_per_second >= spsc.events_per_second * 0.85;
    const bool batch_target = batch_sixteen.events_per_second >=
                              batch_one.events_per_second * 1.5;
    const bool multicast_target = multicast.events_per_second >=
                                  three_queues.events_per_second * 1.25;

    std::cout << "\nAcceptance (single run; use repeated pinned runs for claims)\n"
              << "correct checksums:        " << (correct ? "PASS" : "FAIL")
              << '\n'
              << ">=2x mutex:              "
              << (mutex_target ? "PASS" : "FAIL") << '\n'
              << "within 15% of SPSC:      "
              << (spsc_target ? "PASS" : "FAIL") << '\n'
              << "batch16 >=1.5x batch1:   "
              << (batch_target ? "PASS" : "FAIL") << '\n'
              << "multicast >=1.25x 3 SPSC: "
              << (multicast_target ? "PASS" : "FAIL") << '\n';

    return correct && mutex_target && spsc_target && batch_target &&
                   multicast_target
               ? 0
               : 1;
}
