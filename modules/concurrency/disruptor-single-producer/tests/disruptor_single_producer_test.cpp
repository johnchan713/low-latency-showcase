#include <lls/concurrency/disruptor_single_producer.hpp>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <new>
#include <thread>
#include <vector>

namespace {

struct event final {
    std::uint64_t value{};
};

struct counted_event final {
    counted_event() noexcept { constructions.fetch_add(1, std::memory_order_relaxed); }

    static void* operator new[](std::size_t size) {
        allocations.fetch_add(1, std::memory_order_relaxed);
        return ::operator new[](size);
    }

    static void operator delete[](void* memory) noexcept {
        ::operator delete[](memory);
    }

    std::uint64_t value{};
    inline static std::atomic<std::size_t> constructions{0};
    inline static std::atomic<std::size_t> allocations{0};
};

using small_disruptor =
    lls::concurrency::single_producer_disruptor<event, 4, 2>;

[[nodiscard]] bool basic_contract_test() {
    small_disruptor disruptor;
    bool called = false;
    if (disruptor.try_consume(0, [&called](const event&) noexcept {
            called = true;
        })) {
        return false;
    }
    if (called || disruptor.published_sequence() != -1) {
        return false;
    }

    if (!disruptor.try_publish(
            [](event& output) noexcept { output.value = 42; })) {
        return false;
    }
    for (std::size_t consumer = 0; consumer < 2; ++consumer) {
        std::uint64_t observed = 0;
        if (!disruptor.try_consume(
                consumer,
                [&observed](const event& input) noexcept {
                    observed = input.value;
                })) {
            return false;
        }
        if (observed != 42) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool wrap_and_slowest_consumer_test() {
    small_disruptor disruptor;
    for (std::uint64_t value = 0; value < 4; ++value) {
        if (!disruptor.try_publish([value](event& output) noexcept {
                output.value = value;
            })) {
            return false;
        }
    }

    if (disruptor.try_publish(
            [](event& output) noexcept { output.value = 99; })) {
        return false;
    }

    for (std::size_t consumer = 0; consumer < 2; ++consumer) {
        const auto count = disruptor.consume_available(
            consumer,
            4,
            [](const event&, std::int64_t) noexcept {});
        if (count != 4) {
            return false;
        }
        if (consumer == 0 && disruptor.try_publish(
                                 [](event& output) noexcept {
                                     output.value = 99;
                                 })) {
            return false;
        }
    }

    std::uint64_t observed = 0;
    if (!disruptor.try_publish(
            [](event& output) noexcept { output.value = 4; })) {
        return false;
    }
    if (!disruptor.try_consume(0, [&observed](const event& input) noexcept {
            observed = input.value;
        })) {
        return false;
    }
    return observed == 4;
}

[[nodiscard]] bool batch_wrap_test() {
    small_disruptor disruptor;
    if (!disruptor.try_publish_batch(
            4,
            [](event& output, std::size_t index) noexcept {
                output.value = index;
            })) {
        return false;
    }
    for (std::size_t consumer = 0; consumer < 2; ++consumer) {
        std::uint64_t expected = 0;
        const auto count = disruptor.consume_available(
            consumer,
            4,
            [&expected](const event& input, std::int64_t) noexcept {
                if (input.value == expected) {
                    ++expected;
                }
            });
        if (count != 4 || expected != 4) {
            return false;
        }
    }
    return disruptor.try_publish_batch(
        2,
        [](event& output, std::size_t index) noexcept {
            output.value = index + 4;
        });
}

[[nodiscard]] bool preallocation_test() {
    counted_event::constructions.store(0, std::memory_order_relaxed);
    counted_event::allocations.store(0, std::memory_order_relaxed);
    using stream_type = lls::concurrency::single_producer_disruptor<
        counted_event,
        16,
        1>;
    stream_type disruptor;
    if (counted_event::constructions.load(std::memory_order_relaxed) != 16) {
        return false;
    }

    const auto allocations_before =
        counted_event::allocations.load(std::memory_order_relaxed);
    for (std::uint64_t value = 0; value < 1'000; ++value) {
        while (!disruptor.try_publish([value](counted_event& output) noexcept {
            output.value = value;
        })) {
        }
        std::uint64_t observed = 0;
        if (!disruptor.try_consume(
                0,
                [&observed](const counted_event& input) noexcept {
                    observed = input.value;
                }) ||
            observed != value) {
            return false;
        }
    }
    const auto allocations_after =
        counted_event::allocations.load(std::memory_order_relaxed);
    return allocations_before == allocations_after &&
           counted_event::constructions.load(std::memory_order_relaxed) == 16;
}

[[nodiscard]] bool concurrent_multicast_test() {
    constexpr std::uint64_t event_count = 500'000;
    constexpr std::size_t consumer_count = 3;
    using disruptor_type = lls::concurrency::single_producer_disruptor<
        event,
        1024,
        consumer_count>;

    disruptor_type disruptor;
    std::atomic<bool> start{false};
    std::array<std::uint64_t, consumer_count> sums{};
    std::array<std::thread, consumer_count> consumers;

    for (std::size_t index = 0; index < consumer_count; ++index) {
        consumers[index] = std::thread([&, index] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            std::uint64_t consumed = 0;
            std::uint64_t expected = 0;
            while (consumed < event_count) {
                const auto batch = disruptor.consume_available(
                    index,
                    64,
                    [&expected, &sums, index](
                        const event& input,
                        std::int64_t) noexcept {
                        if (input.value == expected) {
                            sums[index] += input.value;
                        }
                        ++expected;
                    });
                consumed += batch;
                if (batch == 0) {
                    std::this_thread::yield();
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    std::uint64_t published = 0;
    while (published < event_count) {
        const auto remaining = event_count - published;
        const auto count = static_cast<std::size_t>(remaining < 64 ? remaining : 64);
        if (disruptor.try_publish_batch(
                count,
                [published](event& output, std::size_t index) noexcept {
                    output.value = published + index;
                })) {
            published += count;
        } else {
            std::this_thread::yield();
        }
    }

    for (auto& consumer : consumers) {
        consumer.join();
    }

    const auto expected_sum = (event_count - 1) * event_count / 2;
    for (std::size_t index = 0; index < consumer_count; ++index) {
        if (sums[index] != expected_sum ||
            disruptor.consumer_sequence(index) !=
                static_cast<std::int64_t>(event_count - 1)) {
            return false;
        }
    }
    return true;
}

}  // namespace

int main() {
    const std::vector<std::pair<const char*, bool (*)()>> tests{
        {"basic contract", basic_contract_test},
        {"wrap and slowest consumer", wrap_and_slowest_consumer_test},
        {"batch wrap", batch_wrap_test},
        {"preallocation", preallocation_test},
        {"concurrent multicast", concurrent_multicast_test},
    };

    for (const auto& [name, test] : tests) {
        if (!test()) {
            std::cerr << "FAILED: " << name << '\n';
            return 1;
        }
    }
    return 0;
}
