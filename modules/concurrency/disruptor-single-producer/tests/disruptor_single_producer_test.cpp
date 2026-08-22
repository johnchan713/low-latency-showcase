#include <lls/concurrency/disruptor_single_producer.hpp>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <new>
#include <limits>
#include <thread>
#include <type_traits>
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

struct cache_line_event final {
    std::array<std::byte, 64> payload{};
};

struct multiword_event final {
    std::array<std::uint64_t, 8> words{};
};

static_assert(sizeof(cache_line_event) == 64);

using small_disruptor =
    lls::concurrency::single_producer_disruptor<event, 4, 2>;

static_assert(!std::is_copy_constructible_v<small_disruptor::producer_handle>);
static_assert(!std::is_copy_assignable_v<small_disruptor::producer_handle>);
static_assert(
    std::is_nothrow_move_constructible_v<small_disruptor::producer_handle>);
static_assert(!std::is_move_assignable_v<small_disruptor::producer_handle>);

[[nodiscard]] bool basic_contract_test() {
    small_disruptor disruptor;
    bool called = false;
    if (disruptor.try_consume(0, [&called](const event&) noexcept {
            called = true;
        })) {
        return false;
    }
    if (called || disruptor.published_sequence() !=
                      std::numeric_limits<small_disruptor::sequence_type>::max()) {
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
            [](const event&, std::uint64_t) noexcept {});
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
            [&expected](const event& input, std::uint64_t) noexcept {
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

[[nodiscard]] bool batch_argument_boundaries_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 4, 1>;
    constexpr std::uint64_t initial_position = 17;
    disruptor_type disruptor{initial_position};
    std::size_t writer_calls = 0;
    const auto writer = [&writer_calls](event&, std::size_t) noexcept {
        ++writer_calls;
    };

    if (!disruptor.try_publish_batch(0, writer) || writer_calls != 0 ||
        disruptor.published_position() != initial_position ||
        disruptor.consumer_position(0) != initial_position) {
        return false;
    }

    return !disruptor.try_publish_batch(5, writer) && writer_calls == 0 &&
           disruptor.published_position() == initial_position &&
           disruptor.consumer_position(0) == initial_position;
}

[[nodiscard]] bool exact_capacity_and_failed_retry_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 8, 2>;
    disruptor_type disruptor;
    if (!disruptor.try_publish_batch(
            6,
            [](event& output, std::size_t index) noexcept {
                output.value = index;
            }) ||
        disruptor.consume_available(
            0,
            3,
            [](const event&, std::uint64_t) noexcept {}) != 3 ||
        disruptor.consume_available(
            1,
            4,
            [](const event&, std::uint64_t) noexcept {}) != 4) {
        return false;
    }

    std::size_t writer_calls = 0;
    if (!disruptor.try_publish_batch(
            5,
            [&writer_calls](event& output, std::size_t index) noexcept {
                ++writer_calls;
                output.value = index + 6;
            }) ||
        writer_calls != 5 || disruptor.published_position() != 11 ||
        disruptor.consumer_position(0) != 3 ||
        disruptor.consumer_position(1) != 4) {
        return false;
    }

    bool writer_called = false;
    const auto writer = [&writer_called](event&) noexcept {
        writer_called = true;
    };
    if (disruptor.try_publish(writer) || writer_called ||
        disruptor.published_position() != 11 ||
        disruptor.consumer_position(0) != 3 ||
        disruptor.consumer_position(1) != 4) {
        return false;
    }
    if (disruptor.try_publish(writer) || writer_called ||
        disruptor.published_position() != 11 ||
        disruptor.consumer_position(0) != 3 ||
        disruptor.consumer_position(1) != 4) {
        return false;
    }

    if (disruptor.consume_available(
            0,
            4,
            [](const event&, std::uint64_t) noexcept {}) != 4 ||
        !disruptor.try_publish(writer)) {
        return false;
    }
    return writer_called && disruptor.published_position() == 12 &&
           disruptor.consumer_position(0) == 7 &&
           disruptor.consumer_position(1) == 4;
}

[[nodiscard]] bool consumer_handle_physical_wrap_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 8, 1>;
    constexpr std::uint64_t initial_position = 6;
    disruptor_type disruptor{initial_position};
    auto consumer = disruptor.make_consumer<0>();
    if (!disruptor.try_publish_batch(
            4,
            [](event& output, std::size_t index) noexcept {
                output.value = initial_position + index;
            })) {
        return false;
    }

    auto expected = initial_position;
    bool ordered = true;
    const auto count = consumer.consume_available(
        4,
        [&expected, &ordered](const event& input,
                              std::uint64_t sequence) noexcept {
            ordered = ordered && sequence == expected &&
                      input.value == expected;
            ++expected;
        });
    return count == 4 && ordered && expected == initial_position + 4 &&
           consumer.position() == initial_position + 4 &&
           disruptor.consumer_position(0) == initial_position + 4;
}

[[nodiscard]] bool moved_handle_sequence_rollover_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 8, 1>;
    constexpr auto initial_position =
        std::numeric_limits<disruptor_type::sequence_type>::max() - 2;
    disruptor_type disruptor{initial_position};
    auto original = disruptor.make_consumer<0>();
    auto consumer = std::move(original);
    if (!disruptor.try_publish_batch(
            4,
            [](event& output, std::size_t index) noexcept {
                output.value = initial_position + index;
            })) {
        return false;
    }

    auto expected = initial_position;
    bool ordered = true;
    const auto count = consumer.consume_available(
        4,
        [&expected, &ordered](const event& input,
                              std::uint64_t sequence) noexcept {
            ordered = ordered && sequence == expected &&
                      input.value == expected;
            ++expected;
        });
    return count == 4 && ordered && expected == 1 &&
           disruptor.published_position() == 1 &&
           disruptor.consumer_position(0) == 1 && consumer.position() == 1;
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

[[nodiscard]] bool cache_line_storage_alignment_test() {
    using stream_type = lls::concurrency::single_producer_disruptor<
        cache_line_event,
        8,
        1>;
    stream_type disruptor;
    for (std::size_t index = 0; index < 8; ++index) {
        bool aligned = false;
        if (!disruptor.try_publish(
                [&aligned](cache_line_event& output) noexcept {
                    const auto address = reinterpret_cast<std::uintptr_t>(
                        std::addressof(output));
                    aligned = address % 64 == 0;
                }) ||
            !aligned) {
            return false;
        }
        if (!disruptor.try_consume(
                0,
                [](const cache_line_event&) noexcept {})) {
            return false;
        }
    }
    return true;
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
                        std::uint64_t) noexcept {
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
                event_count - 1) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool sequence_rollover_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 4, 2>;
    constexpr auto initial =
        std::numeric_limits<disruptor_type::sequence_type>::max() - 1;
    disruptor_type disruptor{initial};

    if (!disruptor.try_publish_batch(
            4,
            [](event& output, std::size_t index) noexcept {
                output.value = initial + index;
            })) {
        return false;
    }

    for (std::size_t consumer = 0; consumer < 2; ++consumer) {
        auto expected = initial;
        bool ordered = true;
        const auto count = disruptor.consume_available(
            consumer,
            4,
            [&expected, &ordered](const event& input,
                                  std::uint64_t sequence) noexcept {
                ordered = ordered && sequence == expected &&
                          input.value == expected;
                ++expected;
            });
        if (count != 4 || !ordered || expected != initial + 4) {
            return false;
        }
    }

    if (!disruptor.try_publish(
            [](event& output) noexcept { output.value = 2; })) {
        return false;
    }
    for (std::size_t consumer = 0; consumer < 2; ++consumer) {
        std::uint64_t observed = 0;
        if (!disruptor.try_consume(
                consumer,
                [&observed](const event& input) noexcept {
                    observed = input.value;
                }) ||
            observed != 2) {
            return false;
        }
    }
    return disruptor.published_position() == 3 &&
           disruptor.consumer_position(0) == 3 &&
           disruptor.consumer_position(1) == 3;
}

[[nodiscard]] bool consumer_handle_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 64, 1>;
    constexpr std::uint64_t event_count = 200'000;
    disruptor_type disruptor;
    std::atomic<bool> start{false};
    std::atomic<bool> correct{true};
    auto consumer_handle = disruptor.make_consumer<0>();

    std::thread consumer(
        [&, handle = std::move(consumer_handle)]() mutable {
            while (!start.load(std::memory_order_acquire)) {
            }
            std::uint64_t expected = 0;
            while (expected < event_count) {
                const auto consumed = handle.consume_available(
                    64,
                    [&expected, &correct](const event& input,
                                          std::uint64_t sequence) noexcept {
                        if (sequence != expected || input.value != expected) {
                            correct.store(false, std::memory_order_relaxed);
                        }
                        ++expected;
                    });
                if (consumed == 0) {
                    std::this_thread::yield();
                }
            }
        });

    start.store(true, std::memory_order_release);
    std::uint64_t published = 0;
    while (published < event_count) {
        const auto remaining = event_count - published;
        const auto count =
            static_cast<std::size_t>(remaining < 16 ? remaining : 16);
        if (disruptor.try_publish_batch(
                count,
                [published](event& output, std::size_t index) noexcept {
                    output.value = published + index;
                })) {
            published += count;
        }
    }
    consumer.join();
    return correct.load(std::memory_order_relaxed) &&
           disruptor.consumer_position(0) == event_count;
}

[[nodiscard]] bool concurrent_handle_multicast_rollover_test() {
    constexpr std::size_t capacity = 8;
    constexpr std::size_t consumer_count = 2;
    constexpr std::uint64_t event_count = 20'000;
    using disruptor_type = lls::concurrency::single_producer_disruptor<
        event,
        capacity,
        consumer_count>;
    constexpr auto initial_position =
        std::numeric_limits<disruptor_type::sequence_type>::max() - 3;
    disruptor_type disruptor{initial_position};
    std::atomic<bool> start{false};
    std::atomic<bool> release_slow_consumer{false};
    std::atomic<std::size_t> ready{0};
    std::array<std::uint64_t, consumer_count> sums{};
    std::array<bool, consumer_count> ordered{true, true};
    std::array<std::thread, consumer_count> consumers;

    consumers[0] = std::thread([&] {
        auto handle = disruptor.make_consumer<0>();
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        while (!release_slow_consumer.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        std::uint64_t consumed = 0;
        while (consumed < event_count) {
            const auto count = handle.consume_available(
                1,
                [&](const event& input, std::uint64_t sequence) noexcept {
                    ordered[0] = ordered[0] && input.value == consumed &&
                                 sequence == initial_position + consumed;
                    sums[0] += input.value;
                    ++consumed;
                });
            if (count == 0) {
                std::this_thread::yield();
            }
        }
    });

    consumers[1] = std::thread([&] {
        auto handle = disruptor.make_consumer<1>();
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        std::uint64_t consumed = 0;
        while (consumed < event_count) {
            const auto count = handle.consume_available(
                7,
                [&](const event& input, std::uint64_t sequence) noexcept {
                    ordered[1] = ordered[1] && input.value == consumed &&
                                 sequence == initial_position + consumed;
                    sums[1] += input.value;
                    ++consumed;
                });
            if (count == 0) {
                std::this_thread::yield();
            }
        }
    });

    while (ready.load(std::memory_order_acquire) != consumer_count) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    constexpr std::array<std::size_t, 4> batch_pattern{3, 1, 4, 2};
    std::uint64_t published = 0;
    std::uint64_t rejected = 0;
    std::size_t pattern_index = 0;
    while (published < event_count) {
        const auto remaining = event_count - published;
        const auto requested = batch_pattern[pattern_index];
        const auto count = static_cast<std::size_t>(
            remaining < requested ? remaining : requested);
        if (disruptor.try_publish_batch(
                count,
                [published](event& output, std::size_t index) noexcept {
                    output.value = published + index;
                })) {
            published += count;
            pattern_index = (pattern_index + 1) % batch_pattern.size();
        } else {
            ++rejected;
            release_slow_consumer.store(true, std::memory_order_release);
            std::this_thread::yield();
        }
    }
    release_slow_consumer.store(true, std::memory_order_release);

    for (auto& consumer : consumers) {
        consumer.join();
    }

    const auto expected_sum = (event_count - 1) * event_count / 2;
    const auto final_position = initial_position + event_count;
    return rejected > 0 && ordered[0] && ordered[1] &&
           sums[0] == expected_sum && sums[1] == expected_sum &&
           disruptor.published_position() == final_position &&
           disruptor.consumer_position(0) == final_position &&
           disruptor.consumer_position(1) == final_position;
}

[[nodiscard]] bool capacity_one_rollover_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 1, 1>;
    constexpr auto initial =
        std::numeric_limits<disruptor_type::sequence_type>::max();
    disruptor_type disruptor{initial};
    auto consumer = disruptor.make_consumer<0>();

    if (!disruptor.try_publish(
            [](event& output) noexcept { output.value = initial; }) ||
        disruptor.try_publish(
            [](event& output) noexcept { output.value = 0; })) {
        return false;
    }
    std::uint64_t first = 0;
    if (!consumer.try_consume(
            [&first](const event& input) noexcept { first = input.value; }) ||
        first != initial) {
        return false;
    }
    if (!disruptor.try_publish(
            [](event& output) noexcept { output.value = 0; })) {
        return false;
    }
    std::uint64_t second = initial;
    return consumer.try_consume(
               [&second](const event& input) noexcept {
                   second = input.value;
               }) &&
           second == 0 && disruptor.published_position() == 1 &&
           consumer.position() == 1;
}

[[nodiscard]] bool producer_handle_boundaries_and_resumption_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 4, 1>;
    constexpr std::uint64_t initial_position = 7;
    disruptor_type disruptor{initial_position};
    std::size_t writer_calls = 0;
    {
        auto producer = disruptor.make_producer();
        const auto writer = [&writer_calls](event& output,
                                             std::size_t index) noexcept {
            ++writer_calls;
            output.value = initial_position + index;
        };
        if (!producer.try_publish_batch(0, writer) || writer_calls != 0 ||
            producer.position() != initial_position ||
            producer.try_publish_batch(5, writer) || writer_calls != 0 ||
            !producer.try_publish_batch(2, writer) || writer_calls != 2 ||
            producer.position() != initial_position + 2 ||
            disruptor.published_position() != initial_position + 2) {
            return false;
        }
    }

    std::uint64_t expected = initial_position;
    if (disruptor.consume_available(
            0,
            2,
            [&expected](const event& input, std::uint64_t sequence) noexcept {
                if (input.value == expected && sequence == expected) {
                    ++expected;
                }
            }) != 2 ||
        expected != initial_position + 2 ||
        !disruptor.try_publish([](event& output) noexcept {
            output.value = initial_position + 2;
        })) {
        return false;
    }
    {
        auto producer = disruptor.make_producer();
        if (producer.position() != initial_position + 3 ||
            !producer.try_publish([](event& output) noexcept {
                output.value = initial_position + 3;
            }) ||
            disruptor.published_position() != initial_position + 4) {
            return false;
        }
    }

    expected = initial_position + 2;
    return disruptor.consume_available(
               0,
               2,
               [&expected](const event& input,
                           std::uint64_t sequence) noexcept {
                   if (input.value == expected && sequence == expected) {
                       ++expected;
                   }
               }) == 2 &&
           expected == initial_position + 4 &&
           disruptor.consumer_position(0) == initial_position + 4;
}

[[nodiscard]] bool moved_producer_handle_capacity_one_rollover_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 1, 1>;
    constexpr auto initial =
        std::numeric_limits<disruptor_type::sequence_type>::max();
    disruptor_type disruptor{initial};
    auto original = disruptor.make_producer();
    auto producer = std::move(original);
    auto consumer = disruptor.make_consumer<0>();

    if (!producer.try_publish(
            [](event& output) noexcept { output.value = initial; }) ||
        producer.try_publish(
            [](event& output) noexcept { output.value = 0; })) {
        return false;
    }
    std::uint64_t observed = 0;
    if (!consumer.try_consume(
            [&observed](const event& input) noexcept {
                observed = input.value;
            }) ||
        observed != initial ||
        !producer.try_publish(
            [](event& output) noexcept { output.value = 0; })) {
        return false;
    }
    return producer.position() == 1 && disruptor.published_position() == 1;
}

[[nodiscard]] bool producer_session_rollover_resumption_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 2, 1>;
    constexpr auto initial =
        std::numeric_limits<disruptor_type::sequence_type>::max() - 1;
    disruptor_type disruptor{initial};

    {
        auto producer = disruptor.make_producer();
        if (!producer.try_publish_batch(
                2,
                [](event& output, std::size_t index) noexcept {
                    output.value = initial + index;
                }) ||
            producer.position() != 0) {
            return false;
        }
    }

    auto expected = initial;
    const auto verify = [&expected](const event& input,
                                    std::uint64_t sequence) noexcept {
        if (input.value == expected && sequence == expected) {
            ++expected;
        }
    };
    if (disruptor.consume_available(0, 2, verify) != 2 || expected != 0 ||
        !disruptor.try_publish([](event& output) noexcept {
            output.value = 0;
        }) ||
        disruptor.consume_available(0, 1, verify) != 1 || expected != 1) {
        return false;
    }

    {
        auto producer = disruptor.make_producer();
        if (producer.position() != 1 ||
            !producer.try_publish(
                [](event& output) noexcept { output.value = 1; })) {
            return false;
        }
    }

    return disruptor.consume_available(0, 1, verify) == 1 && expected == 2 &&
           disruptor.published_position() == 2 &&
           disruptor.consumer_position(0) == 2;
}

[[nodiscard]] bool producer_handle_slowest_consumer_switch_test() {
    using disruptor_type =
        lls::concurrency::single_producer_disruptor<event, 4, 2>;
    disruptor_type disruptor;
    auto producer = disruptor.make_producer();
    std::array<std::uint64_t, 2> expected{};
    const auto consume = [&](std::size_t consumer,
                             std::size_t count) noexcept {
        return disruptor.consume_available(
            consumer,
            count,
            [&](const event& input, std::uint64_t sequence) noexcept {
                if (input.value == expected[consumer] &&
                    sequence == expected[consumer]) {
                    ++expected[consumer];
                }
            });
    };
    const auto publish = [&producer](std::uint64_t first,
                                     std::size_t count) noexcept {
        return producer.try_publish_batch(
            count,
            [first](event& output, std::size_t index) noexcept {
                output.value = first + index;
            });
    };

    if (!publish(0, 4) || consume(1, 4) != 4 || publish(4, 1) ||
        consume(0, 1) != 1 || !publish(4, 1) || consume(0, 4) != 4 ||
        !publish(5, 3) || publish(8, 1) ||
        consume(1, 4) != 4 || !publish(8, 1) || consume(0, 4) != 4 ||
        consume(1, 1) != 1) {
        return false;
    }

    return expected[0] == 9 && expected[1] == 9 &&
           producer.position() == 9 && disruptor.published_position() == 9 &&
           disruptor.consumer_position(0) == 9 &&
           disruptor.consumer_position(1) == 9;
}

[[nodiscard]] bool concurrent_multiword_producer_handle_test() {
    constexpr std::size_t capacity = 64;
    constexpr std::uint64_t event_count = 250'000;
    using disruptor_type = lls::concurrency::single_producer_disruptor<
        multiword_event,
        capacity,
        1>;
    constexpr auto initial_position =
        std::numeric_limits<disruptor_type::sequence_type>::max() - 31;
    disruptor_type disruptor{initial_position};
    std::atomic<bool> ready{false};
    std::atomic<bool> correct{true};

    std::thread consumer([&] {
        auto handle = disruptor.make_consumer<0>();
        ready.store(true, std::memory_order_release);
        std::uint64_t expected = 0;
        while (expected < event_count) {
            const auto consumed = handle.consume_available(
                32,
                [&](const multiword_event& input,
                    std::uint64_t sequence) noexcept {
                    if (sequence != initial_position + expected) {
                        correct.store(false, std::memory_order_relaxed);
                    }
                    for (std::size_t word = 0; word < input.words.size();
                         ++word) {
                        const auto expected_word =
                            expected * 0x9e3779b97f4a7c15ULL + word;
                        if (input.words[word] != expected_word) {
                            correct.store(false, std::memory_order_relaxed);
                        }
                    }
                    ++expected;
                });
            if (consumed == 0) {
                std::this_thread::yield();
            }
        }
    });

    while (!ready.load(std::memory_order_acquire)) {
    }
    {
        auto producer = disruptor.make_producer();
        constexpr std::array<std::size_t, 4> batches{1, 7, 32, 3};
        std::uint64_t published = 0;
        std::size_t batch_index = 0;
        while (published < event_count) {
            const auto remaining = event_count - published;
            const auto requested = batches[batch_index];
            const auto count = static_cast<std::size_t>(
                remaining < requested ? remaining : requested);
            if (producer.try_publish_batch(
                    count,
                    [published](multiword_event& output,
                                std::size_t index) noexcept {
                        const auto value = published + index;
                        for (std::size_t word = 0; word < output.words.size();
                             ++word) {
                            output.words[word] =
                                value * 0x9e3779b97f4a7c15ULL + word;
                        }
                    })) {
                published += count;
                batch_index = (batch_index + 1) % batches.size();
            } else {
                std::this_thread::yield();
            }
        }
    }
    consumer.join();

    const auto final_position = initial_position + event_count;
    return correct.load(std::memory_order_relaxed) &&
           disruptor.published_position() == final_position &&
           disruptor.consumer_position(0) == final_position;
}

[[nodiscard]] bool concurrent_producer_handle_multicast_rollover_test() {
    constexpr std::size_t capacity = 8;
    constexpr std::size_t consumer_count = 3;
    constexpr std::uint64_t event_count = 100'000;
    using disruptor_type = lls::concurrency::single_producer_disruptor<
        event,
        capacity,
        consumer_count>;
    constexpr auto initial_position =
        std::numeric_limits<disruptor_type::sequence_type>::max() - 5;
    disruptor_type disruptor{initial_position};
    auto producer_handle = disruptor.make_producer();
    std::atomic<bool> start{false};
    std::atomic<bool> release_slow_consumer{false};
    std::atomic<std::size_t> ready{0};
    std::atomic<bool> correct{true};
    std::array<std::uint64_t, consumer_count> sums{};
    std::array<std::thread, consumer_count> consumers;

    consumers[0] = std::thread([&] {
        auto consumer = disruptor.make_consumer<0>();
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
        }
        while (!release_slow_consumer.load(std::memory_order_acquire)) {
        }
        std::uint64_t expected = 0;
        while (expected < event_count) {
            const auto count = consumer.consume_available(
                1,
                [&](const event& input, std::uint64_t sequence) noexcept {
                    if (input.value != expected ||
                        sequence != initial_position + expected) {
                        correct.store(false, std::memory_order_relaxed);
                    }
                    sums[0] += input.value;
                    ++expected;
                });
            if (count == 0) {
                std::this_thread::yield();
            }
        }
    });
    consumers[1] = std::thread([&] {
        auto consumer = disruptor.make_consumer<1>();
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
        }
        std::uint64_t expected = 0;
        while (expected < event_count) {
            const auto count = consumer.consume_available(
                3,
                [&](const event& input, std::uint64_t sequence) noexcept {
                    if (input.value != expected ||
                        sequence != initial_position + expected) {
                        correct.store(false, std::memory_order_relaxed);
                    }
                    sums[1] += input.value;
                    ++expected;
                });
            if (count == 0) {
                std::this_thread::yield();
            }
        }
    });
    consumers[2] = std::thread([&] {
        auto consumer = disruptor.make_consumer<2>();
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
        }
        std::uint64_t expected = 0;
        while (expected < event_count) {
            const auto count = consumer.consume_available(
                7,
                [&](const event& input, std::uint64_t sequence) noexcept {
                    if (input.value != expected ||
                        sequence != initial_position + expected) {
                        correct.store(false, std::memory_order_relaxed);
                    }
                    sums[2] += input.value;
                    ++expected;
                });
            if (count == 0) {
                std::this_thread::yield();
            }
        }
    });

    std::uint64_t rejected = 0;
    std::thread producer(
        [&, handle = std::move(producer_handle)]() mutable {
            while (ready.load(std::memory_order_acquire) != consumer_count) {
            }
            start.store(true, std::memory_order_release);
            constexpr std::array<std::size_t, 4> batches{1, 4, 2, 3};
            std::uint64_t published = 0;
            std::size_t batch_index = 0;
            while (published < event_count) {
                const auto requested = batches[batch_index];
                const auto remaining = event_count - published;
                const auto count = static_cast<std::size_t>(
                    remaining < requested ? remaining : requested);
                if (handle.try_publish_batch(
                        count,
                        [published](event& output,
                                    std::size_t index) noexcept {
                            output.value = published + index;
                        })) {
                    published += count;
                    batch_index = (batch_index + 1) % batches.size();
                } else {
                    ++rejected;
                    release_slow_consumer.store(true,
                                                std::memory_order_release);
                    std::this_thread::yield();
                }
            }
        });

    producer.join();
    for (auto& consumer : consumers) {
        consumer.join();
    }

    const auto expected_sum = (event_count - 1) * event_count / 2;
    const auto final_position = initial_position + event_count;
    return rejected > 0 && correct.load(std::memory_order_relaxed) &&
           sums[0] == expected_sum && sums[1] == expected_sum &&
           sums[2] == expected_sum &&
           disruptor.published_position() == final_position &&
           disruptor.consumer_position(0) == final_position &&
           disruptor.consumer_position(1) == final_position &&
           disruptor.consumer_position(2) == final_position;
}

}  // namespace

int main() {
    const std::vector<std::pair<const char*, bool (*)()>> tests{
        {"basic contract", basic_contract_test},
        {"wrap and slowest consumer", wrap_and_slowest_consumer_test},
        {"batch wrap", batch_wrap_test},
        {"batch argument boundaries", batch_argument_boundaries_test},
        {"exact capacity and failed retry",
         exact_capacity_and_failed_retry_test},
        {"consumer handle physical wrap",
         consumer_handle_physical_wrap_test},
        {"moved handle sequence rollover",
         moved_handle_sequence_rollover_test},
        {"preallocation", preallocation_test},
        {"cache-line storage alignment", cache_line_storage_alignment_test},
        {"concurrent multicast", concurrent_multicast_test},
        {"sequence rollover", sequence_rollover_test},
        {"consumer handle", consumer_handle_test},
        {"concurrent handle multicast rollover",
         concurrent_handle_multicast_rollover_test},
        {"capacity one rollover", capacity_one_rollover_test},
        {"producer handle boundaries and resumption",
         producer_handle_boundaries_and_resumption_test},
        {"moved producer handle capacity-one rollover",
         moved_producer_handle_capacity_one_rollover_test},
        {"producer session rollover resumption",
         producer_session_rollover_resumption_test},
        {"producer handle slowest consumer switch",
         producer_handle_slowest_consumer_switch_test},
        {"concurrent multiword producer handle",
         concurrent_multiword_producer_handle_test},
        {"concurrent producer handle multicast rollover",
         concurrent_producer_handle_multicast_rollover_test},
    };

    for (const auto& [name, test] : tests) {
        if (!test()) {
            std::cerr << "FAILED: " << name << '\n';
            return 1;
        }
    }
    return 0;
}
