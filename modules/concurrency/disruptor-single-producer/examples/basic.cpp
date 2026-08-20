#include <lls/concurrency/disruptor_single_producer.hpp>

#include <atomic>
#include <cstdint>
#include <iostream>
#include <thread>

namespace {

struct market_event final {
    std::uint64_t order_id{};
    std::int64_t price_ticks{};
};

}  // namespace

int main() {
    constexpr std::uint64_t event_count = 100'000;
    using event_stream = lls::concurrency::single_producer_disruptor<
        market_event,
        1024,
        2>;

    event_stream stream;
    std::atomic<bool> start{false};

    std::thread risk([&] {
        auto consumer = stream.make_consumer<0>();
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        std::uint64_t consumed = 0;
        while (consumed < event_count) {
            consumed += consumer.consume_available(
                64,
                [](const market_event& event, std::uint64_t) noexcept {
                    // A real risk handler would update preallocated state here.
                    (void)event;
                });
        }
    });

    std::thread journal([&] {
        auto consumer = stream.make_consumer<1>();
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        std::uint64_t consumed = 0;
        while (consumed < event_count) {
            consumed += consumer.consume_available(
                64,
                [](const market_event& event, std::uint64_t) noexcept {
                    // A real journal would append to a separately managed sink.
                    (void)event;
                });
        }
    });

    start.store(true, std::memory_order_release);
    std::uint64_t published = 0;
    while (published < event_count) {
        const auto remaining = event_count - published;
        const auto batch = static_cast<std::size_t>(remaining < 64 ? remaining : 64);
        if (stream.try_publish_batch(
                batch,
                [published](market_event& event, std::size_t offset) noexcept {
                    event.order_id = published + offset;
                    event.price_ticks = 10'000 +
                                        static_cast<std::int64_t>(offset);
                })) {
            published += batch;
        } else {
            // Backpressure: every consumer must release a slot before reuse.
            std::this_thread::yield();
        }
    }

    risk.join();
    journal.join();
    std::cout << "multicast " << event_count << " events to 2 consumers\n";
}
