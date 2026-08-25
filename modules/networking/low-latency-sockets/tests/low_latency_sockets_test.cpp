#include <lls/networking/low_latency_sockets.hpp>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string_view>
#include <thread>
#include <utility>

namespace {

using namespace std::chrono_literals;
namespace networking = lls::networking;

class unique_fd final {
public:
    unique_fd() = default;
    explicit unique_fd(int descriptor) noexcept : descriptor_(descriptor) {}

    unique_fd(const unique_fd&) = delete;
    unique_fd& operator=(const unique_fd&) = delete;

    unique_fd(unique_fd&& other) noexcept
        : descriptor_(std::exchange(other.descriptor_, -1)) {}

    unique_fd& operator=(unique_fd&& other) noexcept {
        if (this != &other) {
            reset();
            descriptor_ = std::exchange(other.descriptor_, -1);
        }
        return *this;
    }

    ~unique_fd() { reset(); }

    [[nodiscard]] int get() const noexcept { return descriptor_; }
    [[nodiscard]] bool valid() const noexcept { return descriptor_ >= 0; }

    void reset() noexcept {
        if (valid()) {
            static_cast<void>(::close(descriptor_));
            descriptor_ = -1;
        }
    }

private:
    int descriptor_{-1};
};

struct tcp_pair final {
    unique_fd client;
    unique_fd server;

    [[nodiscard]] bool valid() const noexcept {
        return client.valid() && server.valid();
    }
};

struct udp_pair final {
    unique_fd first;
    sockaddr_in first_endpoint{};
    unique_fd second;
    sockaddr_in second_endpoint{};

    [[nodiscard]] bool valid() const noexcept {
        return first.valid() && second.valid();
    }
};

[[nodiscard]] bool check(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
    }
    return condition;
}

[[nodiscard]] sockaddr_in loopback_address() noexcept {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    return address;
}

[[nodiscard]] bool bind_loopback(int descriptor,
                                 sockaddr_in& endpoint) noexcept {
    auto requested = loopback_address();
    if (::bind(descriptor,
               reinterpret_cast<const sockaddr*>(&requested),
               sizeof(requested)) != 0) {
        return false;
    }

    socklen_t length = sizeof(endpoint);
    return ::getsockname(descriptor,
                         reinterpret_cast<sockaddr*>(&endpoint),
                         &length) == 0 &&
           length == sizeof(endpoint);
}

[[nodiscard]] bool connect_to(int descriptor,
                              const sockaddr_in& endpoint) noexcept {
    return ::connect(descriptor,
                     reinterpret_cast<const sockaddr*>(&endpoint),
                     sizeof(endpoint)) == 0;
}

[[nodiscard]] tcp_pair make_tcp_pair() noexcept {
    unique_fd listener{
        ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP)};
    if (!listener.valid()) {
        return {};
    }

    sockaddr_in endpoint{};
    if (!bind_loopback(listener.get(), endpoint) ||
        ::listen(listener.get(), 1) != 0) {
        return {};
    }

    unique_fd client{
        ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP)};
    if (!client.valid() || !connect_to(client.get(), endpoint)) {
        return {};
    }

    unique_fd server{::accept4(listener.get(), nullptr, nullptr, SOCK_CLOEXEC)};
    if (!server.valid()) {
        return {};
    }
    return {std::move(client), std::move(server)};
}

[[nodiscard]] udp_pair make_udp_pair() noexcept {
    unique_fd first{
        ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, IPPROTO_UDP)};
    unique_fd second{
        ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, IPPROTO_UDP)};
    if (!first.valid() || !second.valid()) {
        return {};
    }

    sockaddr_in first_endpoint{};
    sockaddr_in second_endpoint{};
    if (!bind_loopback(first.get(), first_endpoint) ||
        !bind_loopback(second.get(), second_endpoint)) {
        return {};
    }

    return {std::move(first),
            first_endpoint,
            std::move(second),
            second_endpoint};
}

[[nodiscard]] bool same_endpoint(const sockaddr_in& left,
                                 const sockaddr_in& right) noexcept {
    return left.sin_family == right.sin_family &&
           left.sin_port == right.sin_port &&
           left.sin_addr.s_addr == right.sin_addr.s_addr;
}

template <std::size_t Size>
[[nodiscard]] std::array<std::byte, Size> make_payload() noexcept {
    std::array<std::byte, Size> payload{};
    for (std::size_t index = 0; index < payload.size(); ++index) {
        const auto value = static_cast<unsigned char>(
            (index * 37U + Size) & 0xffU);
        payload[index] = static_cast<std::byte>(value);
    }
    return payload;
}

template <std::size_t Size>
[[nodiscard]] bool equal_payload(
    const std::array<std::byte, Size>& left,
    const std::array<std::byte, Size>& right) noexcept {
    return std::equal(left.begin(), left.end(), right.begin());
}

[[nodiscard]] bool socket_option_test() {
    const auto invalid_observation = networking::tcp_no_delay(-1);
    const auto invalid_configuration =
        networking::configure_tcp_no_delay(-1, true);
    const auto invalid_rearm = networking::rearm_tcp_quick_ack(-1);
    const auto bad_descriptor =
        std::make_error_code(std::errc::bad_file_descriptor);
    if (!check(!invalid_observation &&
                   invalid_observation.error() == bad_descriptor &&
                   !invalid_configuration &&
                   invalid_configuration.error() == bad_descriptor &&
                   !invalid_rearm &&
                   invalid_rearm.error() == bad_descriptor,
               "propagate socket option descriptor failures")) {
        return false;
    }

    auto sockets = make_tcp_pair();
    if (!check(sockets.valid(), "create TCP pair for option tests")) {
        return false;
    }

    const auto initial_client =
        networking::tcp_no_delay(sockets.client.get());
    const auto initial_server =
        networking::tcp_no_delay(sockets.server.get());
    if (!check(initial_client.has_value() && initial_server.has_value(),
               "observe TCP_NODELAY without changing it")) {
        return false;
    }

    const auto enabled_client =
        networking::configure_tcp_no_delay(sockets.client.get(), true);
    const auto enabled_server =
        networking::configure_tcp_no_delay(sockets.server.get(), true);
    if (!check(enabled_client.value_or(false) &&
                   enabled_server.value_or(false),
               "enable and read back TCP_NODELAY on both endpoints")) {
        return false;
    }

    const auto disabled_client =
        networking::configure_tcp_no_delay(sockets.client.get(), false);
    const auto disabled_server =
        networking::configure_tcp_no_delay(sockets.server.get(), false);
    if (!check(disabled_client.has_value() && !*disabled_client &&
                   disabled_server.has_value() && !*disabled_server,
               "disable and read back TCP_NODELAY on both endpoints")) {
        return false;
    }

    if (!check(networking::rearm_tcp_quick_ack(sockets.client.get())
                   .has_value() &&
                   networking::rearm_tcp_quick_ack(sockets.server.get())
                       .has_value(),
               "rearm TCP_QUICKACK on connected endpoints")) {
        return false;
    }

    const auto invalid_budget = networking::configure_busy_poll_budget(
        sockets.client.get(), std::chrono::microseconds{-1});
    if (!check(!invalid_budget.has_value() &&
                   invalid_budget.error() ==
                       std::make_error_code(std::errc::invalid_argument),
               "reject a negative SO_BUSY_POLL budget")) {
        return false;
    }

    const auto observed_budget =
        networking::busy_poll_budget(sockets.client.get());
    if (!observed_budget.has_value()) {
        return check(
            observed_budget.error() ==
                    std::make_error_code(std::errc::no_protocol_option) ||
                observed_budget.error() ==
                    std::make_error_code(std::errc::operation_not_supported),
            "report an unsupported SO_BUSY_POLL kernel cleanly");
    }
    const auto zero_budget = networking::configure_busy_poll_budget(
        sockets.client.get(), std::chrono::microseconds{0});
    return check(zero_budget.has_value() &&
                     *zero_budget == std::chrono::microseconds{0},
                 "apply and read back a supported SO_BUSY_POLL budget");
}

[[nodiscard]] bool tcp_blocking_and_partial_close_test() {
    auto sockets = make_tcp_pair();
    if (!check(sockets.valid(), "create TCP pair for blocking receive")) {
        return false;
    }

    const auto before = networking::tcp_no_delay(sockets.client.get());
    const auto payload = make_payload<64>();
    const auto first = std::span<const std::byte>{payload}.first<13>();
    const auto second = std::span<const std::byte>{payload}.subspan<13>();
    if (!check(networking::send_all(sockets.server.get(), first)
                       .complete(first.size()) &&
                   networking::send_all(sockets.server.get(), second)
                       .complete(second.size()),
               "send fragmented TCP payload")) {
        return false;
    }

    std::array<std::byte, 64> received{};
    const auto receive_result = networking::receive_exact_blocking(
        sockets.client.get(), received);
    const auto after = networking::tcp_no_delay(sockets.client.get());
    if (!check(receive_result.complete() &&
                   receive_result.bytes_received == received.size() &&
                   equal_payload(payload, received),
               "reassemble exact fragmented TCP payload")) {
        return false;
    }
    if (!check(before.has_value() && after.has_value() && *before == *after,
               "ordinary I/O preserves the TCP_NODELAY baseline")) {
        return false;
    }

    auto closing_sockets = make_tcp_pair();
    if (!check(closing_sockets.valid(),
               "create TCP pair for partial peer close")) {
        return false;
    }
    const auto prefix = first.first<9>();
    if (!check(networking::send_all(closing_sockets.server.get(), prefix)
                   .complete(prefix.size()),
               "send TCP prefix before closing peer")) {
        return false;
    }
    closing_sockets.server.reset();

    std::array<std::byte, 64> partial{};
    const auto closed = networking::receive_exact_blocking(
        closing_sockets.client.get(), partial);
    return check(closed.state ==
                         networking::stream_receive_state::peer_closed &&
                     closed.bytes_received == prefix.size() &&
                     std::equal(prefix.begin(), prefix.end(), partial.begin()),
                 "report peer close and preserve partial TCP progress");
}

[[nodiscard]] bool tcp_busy_spin_test() {
    auto sockets = make_tcp_pair();
    if (!check(sockets.valid(), "create TCP pair for busy-spin receive")) {
        return false;
    }

    const int flags_before = ::fcntl(sockets.client.get(), F_GETFL, 0);
    if (!check(flags_before >= 0, "read descriptor flags before TCP spin")) {
        return false;
    }

    const auto payload = make_payload<256>();
    networking::send_result sender_result{};
    std::atomic<bool> receiver_polled{false};
    std::thread sender{[&] {
        while (!receiver_polled.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        sender_result = networking::send_all(sockets.server.get(), payload);
    }};

    std::array<std::byte, 256> received{};
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    const auto receive_result = networking::receive_exact_busy_spin(
        sockets.client.get(),
        received,
        [&] {
            receiver_polled.store(true, std::memory_order_release);
            return std::chrono::steady_clock::now() >= deadline;
        });
    sender.join();

    const int flags_after = ::fcntl(sockets.client.get(), F_GETFL, 0);
    if (!check(sender_result.complete(payload.size()) &&
                   receive_result.complete() &&
                   equal_payload(payload, received),
               "receive TCP data arriving during busy spin")) {
        return false;
    }
    if (!check(flags_after == flags_before &&
                   (flags_after & O_NONBLOCK) == 0,
               "MSG_DONTWAIT spin preserves descriptor blocking mode")) {
        return false;
    }

    const auto deadline_payload = make_payload<8>();
    if (!check(networking::send_all(
                       sockets.server.get(), deadline_payload)
                   .complete(deadline_payload.size()),
               "send TCP data before a chrono deadline")) {
        return false;
    }
    std::array<std::byte, 8> deadline_received{};
    const auto before_deadline =
        networking::receive_exact_busy_spin_until(
            sockets.client.get(),
            deadline_received,
            std::chrono::steady_clock::now() + 5s);
    if (!check(before_deadline.complete() &&
                   equal_payload(deadline_payload, deadline_received),
               "receive TCP data before a chrono deadline")) {
        return false;
    }

    std::array<std::byte, 8> silent_buffer{};
    std::size_t polls{};
    const auto stopped = networking::receive_exact_busy_spin(
        sockets.client.get(), silent_buffer, [&polls] {
            ++polls;
            return polls >= 64U;
        });
    if (!check(stopped.state == networking::stream_receive_state::stopped &&
                   stopped.bytes_received == 0 && polls == 64U,
               "stop a silent TCP busy-spin receive deterministically")) {
        return false;
    }

    const auto stopped_until = networking::receive_exact_busy_spin_until(
        sockets.client.get(),
        silent_buffer,
        std::chrono::steady_clock::now() - 1ns);
    return check(
        stopped_until.state == networking::stream_receive_state::stopped &&
            stopped_until.bytes_received == 0,
        "adapt a chrono deadline for TCP busy-spin receive");
}

[[nodiscard]] bool tcp_busy_spin_partial_progress_test() {
    auto sockets = make_tcp_pair();
    if (!check(sockets.valid(),
               "create TCP pair for partial busy-spin receive")) {
        return false;
    }

    const auto payload = make_payload<64>();
    constexpr std::size_t prefix_size = 9;
    const auto prefix =
        std::span<const std::byte>{payload}.first(prefix_size);
    if (!check(networking::send_all(sockets.server.get(), prefix)
                   .complete(prefix.size()),
               "send a prefix before stopping TCP busy spin")) {
        return false;
    }

    std::array<std::byte, 64> received{};
    const auto stopped = networking::receive_exact_busy_spin(
        sockets.client.get(), received, [] { return true; });
    if (!check(stopped.state == networking::stream_receive_state::stopped &&
                   stopped.bytes_received == prefix.size() &&
                   std::equal(prefix.begin(), prefix.end(), received.begin()),
               "preserve partial TCP bytes when busy spin stops")) {
        return false;
    }

    const auto remainder =
        std::span<const std::byte>{payload}.subspan(prefix_size);
    if (!check(networking::send_all(sockets.server.get(), remainder)
                   .complete(remainder.size()),
               "send the remainder after stopping TCP busy spin")) {
        return false;
    }
    auto received_remainder =
        std::span<std::byte>{received}.subspan(prefix_size);
    const auto resumed = networking::receive_exact_busy_spin_until(
        sockets.client.get(),
        received_remainder,
        std::chrono::steady_clock::now() + 5s);
    if (!check(resumed.complete() &&
                   resumed.bytes_received == remainder.size() &&
                   equal_payload(payload, received),
               "resume TCP receive without corrupting the message")) {
        return false;
    }

    auto closing_sockets = make_tcp_pair();
    if (!check(closing_sockets.valid(),
               "create TCP pair for partial busy-spin peer close")) {
        return false;
    }
    if (!check(networking::send_all(closing_sockets.server.get(), prefix)
                   .complete(prefix.size()),
               "send a prefix before closing a busy-spin peer")) {
        return false;
    }
    closing_sockets.server.reset();

    std::array<std::byte, 64> partial{};
    const auto closed = networking::receive_exact_busy_spin_until(
        closing_sockets.client.get(),
        partial,
        std::chrono::steady_clock::now() + 5s);
    return check(
        closed.state == networking::stream_receive_state::peer_closed &&
            closed.bytes_received == prefix.size() &&
            std::equal(prefix.begin(), prefix.end(), partial.begin()),
        "report busy-spin peer close with exact partial progress");
}

[[nodiscard]] bool udp_unconnected_test() {
    auto sockets = make_udp_pair();
    if (!check(sockets.valid(), "create unconnected UDP pair")) {
        return false;
    }

    std::array<std::byte, 8> empty_buffer{};
    const auto not_ready = networking::try_receive_datagram(
        sockets.second.get(), empty_buffer);
    if (!check(not_ready.state ==
                   networking::datagram_receive_state::not_ready,
               "empty nonblocking UDP receive reports not-ready")) {
        return false;
    }

    const auto small_payload = make_payload<8>();
    const auto small_send = networking::send_datagram_to(
        sockets.first.get(), small_payload, sockets.second_endpoint);
    std::array<std::byte, 8> small_received{};
    const auto small_receive = networking::receive_datagram_blocking(
        sockets.second.get(), small_received);
    if (!check(small_send.complete(small_payload.size()) &&
                   small_receive.received() &&
                   small_receive.wire_bytes == small_payload.size() &&
                   small_receive.peer_length == sizeof(sockaddr_in) &&
                   !small_receive.truncated(small_received.size()) &&
                   same_endpoint(small_receive.peer,
                                 sockets.first_endpoint) &&
                   equal_payload(small_payload, small_received),
               "preserve payload and peer for unconnected UDP")) {
        return false;
    }

    const auto large_payload = make_payload<1400>();
    const auto large_send = networking::send_datagram_to(
        sockets.first.get(), large_payload, sockets.second_endpoint);
    std::array<std::byte, 1400> large_received{};
    const auto large_receive = networking::receive_datagram_blocking(
        sockets.second.get(), large_received);
    if (!check(large_send.complete(large_payload.size()) &&
                   large_receive.received() &&
                   large_receive.wire_bytes == large_payload.size() &&
                   large_receive.peer_length == sizeof(sockaddr_in) &&
                   !large_receive.truncated(large_received.size()) &&
                   same_endpoint(large_receive.peer,
                                 sockets.first_endpoint) &&
                   equal_payload(large_payload, large_received),
               "transfer an unconnected near-MTU UDP payload")) {
        return false;
    }

    const auto oversized_payload = make_payload<64>();
    if (!check(networking::send_datagram_to(
                       sockets.first.get(),
                       oversized_payload,
                       sockets.second_endpoint)
                   .complete(oversized_payload.size()),
               "send UDP datagram larger than receive buffer")) {
        return false;
    }
    std::array<std::byte, 8> truncated_buffer{};
    const auto truncated = networking::receive_datagram_blocking(
        sockets.second.get(), truncated_buffer);
    return check(truncated.received() &&
                     truncated.wire_bytes == oversized_payload.size() &&
                     truncated.peer_length == sizeof(sockaddr_in) &&
                     truncated.truncated(truncated_buffer.size()) &&
                     same_endpoint(truncated.peer,
                                   sockets.first_endpoint) &&
                     std::equal(truncated_buffer.begin(),
                                truncated_buffer.end(),
                                oversized_payload.begin()),
                 "surface UDP truncation and original wire length");
}

[[nodiscard]] bool udp_connected_busy_spin_test() {
    auto sockets = make_udp_pair();
    if (!check(sockets.valid(), "create connected UDP pair")) {
        return false;
    }
    if (!check(connect_to(sockets.first.get(), sockets.second_endpoint) &&
                   connect_to(sockets.second.get(), sockets.first_endpoint),
               "connect both UDP endpoints")) {
        return false;
    }

    std::array<std::byte, 8> empty_buffer{};
    const auto not_ready = networking::try_receive_connected_datagram(
        sockets.second.get(), empty_buffer);
    if (!check(not_ready.state ==
                   networking::datagram_receive_state::not_ready,
               "empty connected UDP fast receive reports not-ready")) {
        return false;
    }

    const int flags_before = ::fcntl(sockets.second.get(), F_GETFL, 0);
    if (!check(flags_before >= 0, "read descriptor flags before UDP spin")) {
        return false;
    }

    const auto payload = make_payload<1400>();
    networking::send_result sender_result{};
    std::atomic<bool> receiver_polled{false};
    std::thread sender{[&] {
        while (!receiver_polled.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        sender_result =
            networking::send_connected_datagram(sockets.first.get(), payload);
    }};

    std::array<std::byte, 1400> received{};
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    const auto receive_result =
        networking::receive_connected_datagram_busy_spin(
        sockets.second.get(),
        received,
        [&] {
            receiver_polled.store(true, std::memory_order_release);
            return std::chrono::steady_clock::now() >= deadline;
        });
    sender.join();

    const int flags_after = ::fcntl(sockets.second.get(), F_GETFL, 0);
    if (!check(sender_result.complete(payload.size()) &&
                   receive_result.received() &&
                   receive_result.wire_bytes == payload.size() &&
                   !receive_result.truncated(received.size()) &&
                   equal_payload(payload, received),
               "receive connected UDP data arriving during busy spin")) {
        return false;
    }

    const auto reply = make_payload<8>();
    if (!check(networking::send_connected_datagram(
                       sockets.second.get(), reply)
                   .complete(reply.size()),
               "send a connected UDP reply")) {
        return false;
    }
    std::array<std::byte, 8> reply_received{};
    const auto reply_result =
        networking::receive_connected_datagram_blocking(
            sockets.first.get(), reply_received);
    if (!check(reply_result.received() &&
                   reply_result.wire_bytes == reply.size() &&
                   equal_payload(reply, reply_received),
               "transfer an 8-byte connected UDP reply")) {
        return false;
    }

    if (!check(networking::send_connected_datagram(
                       sockets.first.get(), reply)
                   .complete(reply.size()),
               "send connected UDP data before a chrono deadline")) {
        return false;
    }
    std::array<std::byte, 8> deadline_received{};
    const auto before_deadline =
        networking::receive_connected_datagram_busy_spin_until(
            sockets.second.get(),
            deadline_received,
            std::chrono::steady_clock::now() + 5s);
    if (!check(before_deadline.received() &&
                   before_deadline.wire_bytes == reply.size() &&
                   equal_payload(reply, deadline_received),
               "receive connected UDP data before a chrono deadline")) {
        return false;
    }
    if (!check(flags_after == flags_before &&
                   (flags_after & O_NONBLOCK) == 0,
               "UDP spin preserves descriptor blocking mode")) {
        return false;
    }

    const auto oversized = make_payload<64>();
    if (!check(networking::send_connected_datagram(
                       sockets.first.get(), oversized)
                   .complete(oversized.size()),
               "send oversized connected UDP datagram")) {
        return false;
    }
    std::array<std::byte, 8> truncated_buffer{};
    const auto truncated =
        networking::receive_connected_datagram_blocking(
            sockets.second.get(), truncated_buffer);
    if (!check(truncated.received() &&
                   truncated.wire_bytes == oversized.size() &&
                   truncated.truncated(truncated_buffer.size()) &&
                   std::equal(truncated_buffer.begin(),
                              truncated_buffer.end(),
                              oversized.begin()),
               "preserve connected UDP wire length on truncation")) {
        return false;
    }

    std::array<std::byte, 8> silent_buffer{};
    std::size_t polls{};
    const auto stopped =
        networking::receive_connected_datagram_busy_spin(
        sockets.second.get(), silent_buffer, [&polls] {
            ++polls;
            return polls >= 64U;
        });
    if (!check(stopped.state ==
                       networking::datagram_receive_state::stopped &&
                   stopped.wire_bytes == 0 && polls == 64U,
               "stop a silent UDP busy-spin receive deterministically")) {
        return false;
    }

    const auto stopped_until =
        networking::receive_connected_datagram_busy_spin_until(
            sockets.second.get(),
            silent_buffer,
            std::chrono::steady_clock::now() - 1ns);
    return check(
        stopped_until.state ==
                networking::datagram_receive_state::stopped &&
            stopped_until.wire_bytes == 0,
        "adapt a chrono deadline for UDP busy-spin receive");
}

[[nodiscard]] bool error_and_empty_buffer_test() {
    const auto payload = make_payload<8>();
    std::array<std::byte, 8> buffer{};

    const auto send_error = networking::send_all(-1, payload);
    const auto stream_error =
        networking::receive_exact_blocking(-1, buffer);
    const auto datagram_error =
        networking::try_receive_datagram(-1, buffer);
    const auto connected_datagram_error =
        networking::try_receive_connected_datagram(-1, buffer);
    if (!check(send_error.error ==
                       std::make_error_code(std::errc::bad_file_descriptor) &&
                   stream_error.state ==
                       networking::stream_receive_state::error &&
                   stream_error.error ==
                       std::make_error_code(std::errc::bad_file_descriptor) &&
                   datagram_error.state ==
                       networking::datagram_receive_state::error &&
                   datagram_error.error ==
                       std::make_error_code(std::errc::bad_file_descriptor) &&
                   connected_datagram_error.state ==
                       networking::datagram_receive_state::error &&
                   connected_datagram_error.error ==
                       std::make_error_code(std::errc::bad_file_descriptor),
               "return errno and progress for socket failures")) {
        return false;
    }

    auto reset_sockets = make_tcp_pair();
    if (!check(reset_sockets.valid(),
               "create TCP pair for closed-peer send")) {
        return false;
    }
    const linger reset_on_close{1, 0};
    if (!check(::setsockopt(reset_sockets.server.get(),
                            SOL_SOCKET,
                            SO_LINGER,
                            &reset_on_close,
                            sizeof(reset_on_close)) == 0,
               "configure deterministic TCP reset")) {
        return false;
    }
    reset_sockets.server.reset();
    std::array<std::byte, 1> reset_buffer{};
    const auto reset_receive = networking::receive_exact_blocking(
        reset_sockets.client.get(), reset_buffer);
    if (!check(reset_receive.state ==
                       networking::stream_receive_state::error &&
                   reset_receive.error ==
                       std::make_error_code(std::errc::connection_reset),
               "observe a reset TCP peer")) {
        return false;
    }
    const auto closed_peer_send =
        networking::send_all(reset_sockets.client.get(), payload);
    if (!check(!closed_peer_send.complete(payload.size()) &&
                   (closed_peer_send.error ==
                        std::make_error_code(std::errc::broken_pipe) ||
                    closed_peer_send.error ==
                        std::make_error_code(std::errc::connection_reset)),
               "suppress SIGPIPE and return a closed-peer send error")) {
        return false;
    }

    auto sockets = make_tcp_pair();
    if (!check(sockets.valid(), "create TCP pair for empty buffer test")) {
        return false;
    }
    const std::span<const std::byte> empty_send{};
    std::span<std::byte> empty_receive{};
    return check(networking::send_all(sockets.client.get(), empty_send)
                         .complete(0) &&
                     networking::receive_exact_blocking(
                         sockets.server.get(), empty_receive)
                         .complete(),
                 "treat empty exact TCP operations as complete");
}

}  // namespace

int main() {
    if (!socket_option_test() ||
        !tcp_blocking_and_partial_close_test() ||
        !tcp_busy_spin_test() ||
        !tcp_busy_spin_partial_progress_test() ||
        !udp_unconnected_test() ||
        !udp_connected_busy_spin_test() ||
        !error_and_empty_buffer_test()) {
        return 1;
    }

    std::cout << "low-latency socket tests passed\n";
    return 0;
}
