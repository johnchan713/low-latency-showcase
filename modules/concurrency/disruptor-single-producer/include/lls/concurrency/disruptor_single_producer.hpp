#pragma once

#include <array>
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

namespace lls::concurrency {

namespace detail {

// The initial target is x86-64 Linux, where cache lines are normally 64 bytes.
// A named constant also avoids the ABI and compiler-warning concerns of
// std::hardware_destructive_interference_size.
inline constexpr std::size_t cache_line_size = 64;

struct alignas(cache_line_size) padded_sequence final {
    std::atomic<std::int64_t> value{-1};
};

static_assert(sizeof(padded_sequence) == cache_line_size);

}  // namespace detail

/// A bounded, preallocated, single-producer multicast ring buffer.
///
/// Each registered consumer observes every event in publication order. Exactly
/// one thread may publish, and each consumer index must be owned by exactly one
/// thread. Threads are deliberately owned by the caller.
template <typename Event, std::size_t Capacity, std::size_t ConsumerCount>
    requires std::default_initializable<Event>
class single_producer_disruptor final {
    static_assert(Capacity > 0, "Capacity must be non-zero");
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two");
    static_assert(ConsumerCount > 0, "At least one consumer is required");
    static_assert(
        Capacity <= static_cast<std::size_t>(
                        std::numeric_limits<std::int64_t>::max()),
        "Capacity must fit in the signed sequence type");

public:
    using event_type = Event;
    using sequence_type = std::int64_t;

    static constexpr std::size_t capacity = Capacity;
    static constexpr std::size_t consumer_count = ConsumerCount;

    single_producer_disruptor()
        : events_{std::make_unique<Event[]>(Capacity)} {}

    single_producer_disruptor(const single_producer_disruptor&) = delete;
    single_producer_disruptor& operator=(const single_producer_disruptor&) =
        delete;
    single_producer_disruptor(single_producer_disruptor&&) = delete;
    single_producer_disruptor& operator=(single_producer_disruptor&&) = delete;

    /// Mutates and publishes one preconstructed event, or returns false when
    /// the slowest consumer still owns the next ring slot.
    template <typename Writer>
        requires std::is_nothrow_invocable_v<Writer&, Event&>
    [[nodiscard]] bool try_publish(Writer&& writer) noexcept {
        return try_publish_batch(
            1,
            [&writer](Event& event, std::size_t) noexcept {
                std::invoke(writer, event);
            });
    }

    /// Publishes a contiguous all-or-nothing batch. The batch must not exceed
    /// the ring capacity. No event becomes visible until every writer call has
    /// completed.
    template <typename Writer>
        requires std::is_nothrow_invocable_v<Writer&, Event&, std::size_t>
    [[nodiscard]] bool try_publish_batch(std::size_t count,
                                         Writer&& writer) noexcept {
        if (count == 0) {
            return true;
        }
        if (count > Capacity) {
            return false;
        }

        const auto first = next_to_publish_;
        const auto last = first + static_cast<sequence_type>(count) - 1;
        const auto wrap_point = last - static_cast<sequence_type>(Capacity);

        if (wrap_point > cached_slowest_consumer_) {
            cached_slowest_consumer_ = slowest_consumer_sequence();
            if (wrap_point > cached_slowest_consumer_) {
                return false;
            }
        }

        for (std::size_t index = 0; index < count; ++index) {
            const auto sequence =
                first + static_cast<sequence_type>(index);
            std::invoke(writer, event_at(sequence), index);
        }

        next_to_publish_ = last + 1;
        published_.value.store(last, std::memory_order_release);
        return true;
    }

    /// Consumes one event for consumer_index, or returns false if the producer
    /// has not published its next sequence yet.
    template <typename Handler>
        requires std::is_nothrow_invocable_v<Handler&, const Event&>
    [[nodiscard]] bool try_consume(std::size_t consumer_index,
                                   Handler&& handler) noexcept {
        return consume_available(
                   consumer_index,
                   1,
                   [&handler](const Event& event, sequence_type) noexcept {
                       std::invoke(handler, event);
                   }) == 1;
    }

    /// Consumes up to max_count currently available events and advances the
    /// consumer sequence once, after the complete batch has been handled.
    template <typename Handler>
        requires std::is_nothrow_invocable_v<Handler&, const Event&,
                                             sequence_type>
    [[nodiscard]] std::size_t consume_available(
        std::size_t consumer_index,
        std::size_t max_count,
        Handler&& handler) noexcept {
        if (consumer_index >= ConsumerCount || max_count == 0) {
            return 0;
        }

        auto& consumer = consumers_[consumer_index].value;
        const auto first = consumer.load(std::memory_order_relaxed) + 1;
        const auto available = published_.value.load(std::memory_order_acquire);
        if (first > available) {
            return 0;
        }

        const auto available_count = static_cast<std::uint64_t>(
            available - first + 1);
        const auto count = static_cast<std::size_t>(
            available_count < max_count ? available_count : max_count);
        const auto last = first + static_cast<sequence_type>(count) - 1;

        for (auto sequence = first; sequence <= last; ++sequence) {
            std::invoke(handler, std::as_const(event_at(sequence)), sequence);
        }
        consumer.store(last, std::memory_order_release);
        return count;
    }

    [[nodiscard]] sequence_type published_sequence() const noexcept {
        return published_.value.load(std::memory_order_acquire);
    }

    [[nodiscard]] sequence_type consumer_sequence(
        std::size_t consumer_index) const noexcept {
        if (consumer_index >= ConsumerCount) {
            return -1;
        }
        return consumers_[consumer_index].value.load(std::memory_order_acquire);
    }

private:
    [[nodiscard]] Event& event_at(sequence_type sequence) noexcept {
        const auto index = static_cast<std::size_t>(sequence) & (Capacity - 1);
        return events_[index];
    }

    [[nodiscard]] sequence_type slowest_consumer_sequence() const noexcept {
        auto slowest = consumers_.front().value.load(std::memory_order_acquire);
        for (std::size_t index = 1; index < ConsumerCount; ++index) {
            const auto sequence =
                consumers_[index].value.load(std::memory_order_acquire);
            if (sequence < slowest) {
                slowest = sequence;
            }
        }
        return slowest;
    }

    std::unique_ptr<Event[]> events_;
    alignas(detail::cache_line_size) sequence_type next_to_publish_{0};
    sequence_type cached_slowest_consumer_{-1};
    detail::padded_sequence published_{};
    std::array<detail::padded_sequence, ConsumerCount> consumers_{};
};

}  // namespace lls::concurrency
