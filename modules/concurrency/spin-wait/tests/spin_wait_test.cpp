#include <lls/concurrency/spin_wait.hpp>

#include <cstddef>

int main() {
    lls::concurrency::busy_spin_wait busy;
    busy.wait();
    busy.reset();

    lls::concurrency::yield_wait yielding;
    yielding.wait();
    yielding.reset();

    lls::concurrency::adaptive_spin_wait<4> adaptive;
    for (std::size_t count = 0; count < 4; ++count) {
        adaptive.wait();
    }
    if (adaptive.pause_count() != 4 || !adaptive.is_yielding()) {
        return 1;
    }
    adaptive.wait();
    if (!adaptive.is_yielding()) {
        return 1;
    }
    adaptive.reset();
    return adaptive.pause_count() == 0 && !adaptive.is_yielding() ? 0 : 1;
}
