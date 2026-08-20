#pragma once

#include <cstddef>
#include <immintrin.h>
#include <thread>

namespace lls::concurrency {

/// Performs one x86 PAUSE instruction per unsuccessful poll.
/// Suitable when a thread owns a dedicated physical core and minimum handoff
/// latency matters more than CPU consumption.
class busy_spin_wait final {
public:
    void wait() const noexcept { _mm_pause(); }
    void reset() const noexcept {}
};

/// Yields immediately after each unsuccessful poll.
/// Suitable for shared cores, but scheduler intervention raises tail latency.
class yield_wait final {
public:
    void wait() const noexcept { std::this_thread::yield(); }
    void reset() const noexcept {}
};

/// Pauses for PauseLimit unsuccessful polls, then yields until reset.
/// Call reset after useful work so the next idle period begins with PAUSE.
template <std::size_t PauseLimit = 64>
class adaptive_spin_wait final {
    static_assert(PauseLimit > 0, "PauseLimit must be non-zero");

public:
    void wait() noexcept {
        if (pause_count_ < PauseLimit) {
            ++pause_count_;
            _mm_pause();
            return;
        }
        std::this_thread::yield();
    }

    void reset() noexcept { pause_count_ = 0; }

    [[nodiscard]] std::size_t pause_count() const noexcept {
        return pause_count_;
    }

    [[nodiscard]] bool is_yielding() const noexcept {
        return pause_count_ == PauseLimit;
    }

private:
    std::size_t pause_count_{};
};

}  // namespace lls::concurrency
