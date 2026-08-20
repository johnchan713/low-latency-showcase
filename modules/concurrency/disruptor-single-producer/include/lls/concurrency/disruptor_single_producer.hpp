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
    std::atomic<std::uint64_t> value{0};
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
    static_assert(Capacity <=
                      static_cast<std::size_t>(
                          std::numeric_limits<std::uint64_t>::max() / 2),
                  "Capacity must fit within half the sequence range");

public:
    using event_type = Event;
    using sequence_type = std::uint64_t;

    static constexpr std::size_t capacity = Capacity;
    static constexpr std::size_t consumer_count = ConsumerCount;

    /// Thread-owned consumer state. Caching the acquired publication boundary
    /// avoids reloading the shared publication cursor while a previously
    /// acquired range still contains events. A handle must be used by only one
    /// thread at a time and must not outlive its disruptor.
    template <std::size_t ConsumerIndex>
    class consumer_handle final {
        static_assert(ConsumerIndex < ConsumerCount,
                      "Consumer index is outside the registered range");

    public:
        consumer_handle(const consumer_handle&) = delete;
        consumer_handle& operator=(const consumer_handle&) = delete;
        consumer_handle(consumer_handle&& other) noexcept
            : owner_{std::exchange(other.owner_, nullptr)},
              next_position_{other.next_position_},
              cached_published_position_{other.cached_published_position_} {}
        consumer_handle& operator=(consumer_handle&&) = delete;

        template <typename Handler>
            requires std::is_nothrow_invocable_v<Handler&, const Event&,
                                                 sequence_type>
        [[nodiscard]] std::size_t consume_available(
            std::size_t max_count,
            Handler&& handler) noexcept {
            if (max_count == 0) {
                return 0;
            }

            auto available_count = cached_published_position_ - next_position_;
            if (available_count == 0) {
                cached_published_position_ =
                    owner_->published_.value.load(std::memory_order_acquire);
                available_count = cached_published_position_ - next_position_;
                if (available_count == 0) {
                    return 0;
                }
            }

            const auto count = static_cast<std::size_t>(
                available_count < max_count ? available_count : max_count);
            for (std::size_t index = 0; index < count; ++index) {
                const auto sequence =
                    next_position_ + static_cast<sequence_type>(index);
                std::invoke(handler,
                            std::as_const(owner_->event_at(sequence)),
                            sequence);
            }
            next_position_ += static_cast<sequence_type>(count);
            owner_->consumers_[ConsumerIndex].value.store(
                next_position_, std::memory_order_release);
            return count;
        }

        template <typename Handler>
            requires std::is_nothrow_invocable_v<Handler&, const Event&>
        [[nodiscard]] bool try_consume(Handler&& handler) noexcept {
            return consume_available(
                       1,
                       [&handler](const Event& event,
                                  sequence_type) noexcept {
                           std::invoke(handler, event);
                       }) == 1;
        }

        [[nodiscard]] sequence_type position() const noexcept {
            return next_position_;
        }

    private:
        friend class single_producer_disruptor;

        explicit consumer_handle(single_producer_disruptor& owner) noexcept
            : owner_{std::addressof(owner)},
              next_position_{owner.consumers_[ConsumerIndex].value.load(
                  std::memory_order_acquire)},
              cached_published_position_{next_position_} {}

        single_producer_disruptor* owner_;
        sequence_type next_position_;
        sequence_type cached_published_position_;
    };

    explicit single_producer_disruptor(sequence_type initial_position = 0)
        : storage_{std::make_unique<ring_storage>()},
          next_to_publish_{initial_position} {
        published_.value.store(initial_position, std::memory_order_relaxed);
        for (auto& consumer : consumers_) {
            consumer.value.store(initial_position, std::memory_order_relaxed);
        }
    }

    single_producer_disruptor(const single_producer_disruptor&) = delete;
    single_producer_disruptor& operator=(const single_producer_disruptor&) =
        delete;
    single_producer_disruptor(single_producer_disruptor&&) = delete;
    single_producer_disruptor& operator=(single_producer_disruptor&&) = delete;

    template <std::size_t ConsumerIndex>
    [[nodiscard]] consumer_handle<ConsumerIndex> make_consumer() noexcept {
        return consumer_handle<ConsumerIndex>{*this};
    }

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

        if (count > available_before_rescan_ &&
            !refresh_available_capacity(count)) {
            return false;
        }

        const auto first = next_to_publish_;
        for (std::size_t index = 0; index < count; ++index) {
            const auto sequence =
                first + static_cast<sequence_type>(index);
            std::invoke(writer, event_at(sequence), index);
        }

        next_to_publish_ = first + static_cast<sequence_type>(count);
        available_before_rescan_ -= count;
        published_.value.store(next_to_publish_, std::memory_order_release);
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
        const auto first = consumer.load(std::memory_order_relaxed);
        const auto available = published_.value.load(std::memory_order_acquire);
        const auto available_count = available - first;
        if (available_count == 0) {
            return 0;
        }

        const auto count = static_cast<std::size_t>(
            available_count < max_count ? available_count : max_count);

        for (std::size_t index = 0; index < count; ++index) {
            const auto sequence = first + static_cast<sequence_type>(index);
            std::invoke(handler, std::as_const(event_at(sequence)), sequence);
        }
        consumer.store(first + static_cast<sequence_type>(count),
                       std::memory_order_release);
        return count;
    }

    /// Returns the last published sequence modulo 2^64. An empty stream that
    /// starts at position zero therefore reports UINT64_MAX.
    [[nodiscard]] sequence_type published_sequence() const noexcept {
        return published_.value.load(std::memory_order_acquire) - 1;
    }

    /// Returns the unsigned, end-exclusive publication position.
    [[nodiscard]] sequence_type published_position() const noexcept {
        return published_.value.load(std::memory_order_acquire);
    }

    [[nodiscard]] sequence_type consumer_sequence(
        std::size_t consumer_index) const noexcept {
        if (consumer_index >= ConsumerCount) {
            return std::numeric_limits<sequence_type>::max();
        }
        return consumers_[consumer_index].value.load(std::memory_order_acquire) -
               1;
    }

    /// Returns the unsigned, end-exclusive position released by a consumer.
    [[nodiscard]] sequence_type consumer_position(
        std::size_t consumer_index) const noexcept {
        if (consumer_index >= ConsumerCount) {
            return 0;
        }
        return consumers_[consumer_index].value.load(std::memory_order_acquire);
    }

private:
    [[nodiscard]] Event& event_at(sequence_type sequence) noexcept {
        const auto index = static_cast<std::size_t>(sequence) & (Capacity - 1);
        return storage_->events[index];
    }

    [[nodiscard]] bool refresh_available_capacity(
        std::size_t required) noexcept {
        const auto maximum_lag = Capacity - required;
        const auto blocker_position =
            consumers_[blocking_consumer_].value.load(std::memory_order_acquire);
        auto greatest_lag = static_cast<std::size_t>(
            next_to_publish_ - blocker_position);
        if (greatest_lag > maximum_lag) {
            return false;
        }

        auto greatest_lag_index = blocking_consumer_;
        for (std::size_t index = 0; index < ConsumerCount; ++index) {
            if (index == blocking_consumer_) {
                continue;
            }
            const auto position =
                consumers_[index].value.load(std::memory_order_acquire);
            const auto lag = static_cast<std::size_t>(next_to_publish_ - position);
            if (lag > maximum_lag) {
                blocking_consumer_ = index;
                return false;
            }
            if (lag > greatest_lag) {
                greatest_lag = lag;
                greatest_lag_index = index;
            }
        }
        blocking_consumer_ = greatest_lag_index;
        available_before_rescan_ = Capacity - greatest_lag;
        return true;
    }

    struct alignas(detail::cache_line_size) ring_storage final {
        std::array<Event, Capacity> events{};
    };

    static_assert(alignof(ring_storage) >= detail::cache_line_size);

    std::unique_ptr<ring_storage> storage_;
    alignas(detail::cache_line_size) sequence_type next_to_publish_{0};
    std::size_t available_before_rescan_{Capacity};
    std::size_t blocking_consumer_{0};
    detail::padded_sequence published_{};
    std::array<detail::padded_sequence, ConsumerCount> consumers_{};
};

}  // namespace lls::concurrency
