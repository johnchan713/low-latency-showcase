#pragma once

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <lls/concurrency/spin_wait.hpp>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <span>
#include <system_error>

namespace lls::networking {

/// Terminal state of an exact-length stream receive.
enum class stream_receive_state : std::uint8_t {
    complete,
    stopped,
    peer_closed,
    error,
};

struct stream_receive_result final {
    stream_receive_state state{};
    std::size_t bytes_received{};
    std::error_code error{};

    [[nodiscard]] bool complete() const noexcept {
        return state == stream_receive_state::complete;
    }
};

/// Byte progress plus an errno-backed failure for stream or datagram sends.
struct send_result final {
    std::size_t bytes_sent{};
    std::error_code error{};

    [[nodiscard]] bool complete(std::size_t expected_bytes) const noexcept {
        return !error && bytes_sent == expected_bytes;
    }
};

/// Terminal state of one datagram receive attempt or spin loop.
enum class datagram_receive_state : std::uint8_t {
    received,
    not_ready,
    stopped,
    error,
};

struct datagram_receive_result final {
    datagram_receive_state state{};
    std::size_t wire_bytes{};
    sockaddr_in peer{};
    socklen_t peer_length{};
    int message_flags{};
    std::error_code error{};

    [[nodiscard]] bool received() const noexcept {
        return state == datagram_receive_state::received;
    }

    [[nodiscard]] bool truncated(std::size_t buffer_bytes) const noexcept {
        return received() &&
               (((message_flags & MSG_TRUNC) != 0) ||
                wire_bytes > buffer_bytes);
    }
};

namespace detail {

[[nodiscard]] inline std::error_code current_socket_error() noexcept {
    return {errno, std::generic_category()};
}

[[nodiscard]] inline std::expected<int, std::error_code>
integer_socket_option(int descriptor, int level, int option_name) noexcept {
    int value{};
    socklen_t length = sizeof(value);
    if (::getsockopt(
            descriptor, level, option_name, &value, &length) != 0) {
        return std::unexpected(current_socket_error());
    }
    if (length != sizeof(value)) {
        return std::unexpected(
            std::make_error_code(std::errc::protocol_error));
    }
    return value;
}

[[nodiscard]] inline std::expected<void, std::error_code>
set_integer_socket_option(int descriptor,
                          int level,
                          int option_name,
                          int value) noexcept {
    if (::setsockopt(descriptor,
                     level,
                     option_name,
                     &value,
                     sizeof(value)) != 0) {
        return std::unexpected(current_socket_error());
    }
    return {};
}

[[nodiscard]] inline datagram_receive_result receive_datagram_once(
    int descriptor,
    std::span<std::byte> buffer,
    int flags,
    bool retry_interrupted) noexcept {
    datagram_receive_result result{};
    iovec vector{buffer.data(), buffer.size()};
    msghdr message{};
    message.msg_name = &result.peer;
    message.msg_namelen = sizeof(result.peer);
    message.msg_iov = &vector;
    message.msg_iovlen = 1;

    while (true) {
        const auto received =
            ::recvmsg(descriptor, &message, flags | MSG_TRUNC);
        if (received >= 0) {
            result.state = datagram_receive_state::received;
            result.wire_bytes = static_cast<std::size_t>(received);
            result.peer_length = message.msg_namelen;
            result.message_flags = message.msg_flags;
            return result;
        }
        if (errno == EINTR && retry_interrupted) {
            continue;
        }
        if ((flags & MSG_DONTWAIT) != 0 &&
            (errno == EAGAIN || errno == EWOULDBLOCK)) {
            result.state = datagram_receive_state::not_ready;
            return result;
        }
        result.state = datagram_receive_state::error;
        result.error = current_socket_error();
        return result;
    }
}

}  // namespace detail

[[nodiscard]] inline std::expected<bool, std::error_code>
tcp_no_delay(int descriptor) noexcept {
    auto observed = detail::integer_socket_option(
        descriptor, IPPROTO_TCP, TCP_NODELAY);
    if (!observed) {
        return std::unexpected(observed.error());
    }
    return *observed != 0;
}

/// Applies TCP_NODELAY and rejects a kernel readback that differs.
[[nodiscard]] inline std::expected<bool, std::error_code>
configure_tcp_no_delay(int descriptor, bool enabled) noexcept {
    const auto requested = static_cast<int>(enabled);
    auto configured = detail::set_integer_socket_option(
        descriptor, IPPROTO_TCP, TCP_NODELAY, requested);
    if (!configured) {
        return std::unexpected(configured.error());
    }
    auto observed = tcp_no_delay(descriptor);
    if (!observed) {
        return std::unexpected(observed.error());
    }
    if (*observed != enabled) {
        return std::unexpected(
            std::make_error_code(std::errc::protocol_error));
    }
    return *observed;
}

/// Requests Linux's transient quick-ACK mode once; rearm when policy requires.
[[nodiscard]] inline std::expected<void, std::error_code>
rearm_tcp_quick_ack(int descriptor) noexcept {
    return detail::set_integer_socket_option(
        descriptor, IPPROTO_TCP, TCP_QUICKACK, 1);
}

/// Returns the Linux SO_BUSY_POLL budget currently stored on a socket.
[[nodiscard]] inline std::expected<std::chrono::microseconds,
                                   std::error_code>
busy_poll_budget(int descriptor) noexcept {
    auto observed = detail::integer_socket_option(
        descriptor, SOL_SOCKET, SO_BUSY_POLL);
    if (!observed) {
        return std::unexpected(observed.error());
    }
    if (*observed < 0) {
        return std::unexpected(
            std::make_error_code(std::errc::protocol_error));
    }
    return std::chrono::microseconds{*observed};
}

/// Applies SO_BUSY_POLL and rejects invalid or differing kernel readback.
[[nodiscard]] inline std::expected<std::chrono::microseconds,
                                   std::error_code>
configure_busy_poll_budget(
    int descriptor,
    std::chrono::microseconds requested) noexcept {
    if (requested.count() < 0 ||
        requested.count() > std::numeric_limits<int>::max()) {
        return std::unexpected(
            std::make_error_code(std::errc::invalid_argument));
    }
    const auto value = static_cast<int>(requested.count());
    auto configured = detail::set_integer_socket_option(
        descriptor, SOL_SOCKET, SO_BUSY_POLL, value);
    if (!configured) {
        return std::unexpected(configured.error());
    }
    auto observed = busy_poll_budget(descriptor);
    if (!observed) {
        return std::unexpected(observed.error());
    }
    if (*observed != requested) {
        return std::unexpected(
            std::make_error_code(std::errc::protocol_error));
    }
    return *observed;
}

/// Sends an exact stream payload, retaining progress when an error occurs.
[[nodiscard]] inline send_result send_all(
    int descriptor,
    std::span<const std::byte> bytes) noexcept {
    send_result result{};
    while (result.bytes_sent < bytes.size()) {
        const auto sent = ::send(descriptor,
                                 bytes.data() + result.bytes_sent,
                                 bytes.size() - result.bytes_sent,
                                 MSG_NOSIGNAL);
        if (sent > 0) {
            result.bytes_sent += static_cast<std::size_t>(sent);
            continue;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent == 0) {
            result.error =
                std::make_error_code(std::errc::broken_pipe);
            return result;
        }
        result.error = detail::current_socket_error();
        return result;
    }
    return result;
}

/// Blocks until the span is full, the peer closes, or recv fails.
[[nodiscard]] inline stream_receive_result receive_exact_blocking(
    int descriptor,
    std::span<std::byte> bytes) noexcept {
    stream_receive_result result{};
    while (result.bytes_received < bytes.size()) {
        const auto received = ::recv(
            descriptor,
            bytes.data() + result.bytes_received,
            bytes.size() - result.bytes_received,
            0);
        if (received > 0) {
            result.bytes_received += static_cast<std::size_t>(received);
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received == 0) {
            result.state = stream_receive_state::peer_closed;
            return result;
        }
        result.state = stream_receive_state::error;
        result.error = detail::current_socket_error();
        return result;
    }
    result.state = stream_receive_state::complete;
    return result;
}

/// Spins with per-call MSG_DONTWAIT until complete or the idle-path predicate
/// requests a stop. The predicate is also checked after EINTR.
template <typename StopRequested>
[[nodiscard]] stream_receive_result receive_exact_busy_spin(
    int descriptor,
    std::span<std::byte> bytes,
    StopRequested&& stop_requested) {
    stream_receive_result result{};
    lls::concurrency::busy_spin_wait spin_wait;
    while (result.bytes_received < bytes.size()) {
        const auto received = ::recv(
            descriptor,
            bytes.data() + result.bytes_received,
            bytes.size() - result.bytes_received,
            MSG_DONTWAIT);
        if (received > 0) {
            result.bytes_received += static_cast<std::size_t>(received);
            continue;
        }
        if (received < 0 && errno == EINTR) {
            if (static_cast<bool>(stop_requested())) {
                result.state = stream_receive_state::stopped;
                return result;
            }
            continue;
        }
        if (received < 0 &&
            (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (static_cast<bool>(stop_requested())) {
                result.state = stream_receive_state::stopped;
                return result;
            }
            spin_wait.wait();
            continue;
        }
        if (received == 0) {
            result.state = stream_receive_state::peer_closed;
            return result;
        }
        result.state = stream_receive_state::error;
        result.error = detail::current_socket_error();
        return result;
    }
    result.state = stream_receive_state::complete;
    return result;
}

/// Adapts a chrono deadline to the cooperative stream stop predicate.
template <typename Clock, typename Duration>
[[nodiscard]] stream_receive_result receive_exact_busy_spin_until(
    int descriptor,
    std::span<std::byte> bytes,
    std::chrono::time_point<Clock, Duration> deadline) {
    return receive_exact_busy_spin(
        descriptor,
        bytes,
        [deadline] { return Clock::now() >= deadline; });
}

/// Sends one whole datagram on a socket configured with POSIX connect().
[[nodiscard]] inline send_result send_connected_datagram(
    int descriptor,
    std::span<const std::byte> bytes) noexcept {
    send_result result{};
    ssize_t sent{};
    do {
        sent = ::send(
            descriptor, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    } while (sent < 0 && errno == EINTR);
    if (sent < 0) {
        result.error = detail::current_socket_error();
        return result;
    }
    result.bytes_sent = static_cast<std::size_t>(sent);
    if (result.bytes_sent != bytes.size()) {
        result.error = std::make_error_code(std::errc::message_size);
    }
    return result;
}

/// Sends one whole IPv4 datagram to an explicit destination.
[[nodiscard]] inline send_result send_datagram_to(
    int descriptor,
    std::span<const std::byte> bytes,
    const sockaddr_in& destination) noexcept {
    send_result result{};
    ssize_t sent{};
    do {
        sent = ::sendto(
            descriptor,
            bytes.data(),
            bytes.size(),
            MSG_NOSIGNAL,
            reinterpret_cast<const sockaddr*>(&destination),
            sizeof(destination));
    } while (sent < 0 && errno == EINTR);
    if (sent < 0) {
        result.error = detail::current_socket_error();
        return result;
    }
    result.bytes_sent = static_cast<std::size_t>(sent);
    if (result.bytes_sent != bytes.size()) {
        result.error = std::make_error_code(std::errc::message_size);
    }
    return result;
}

/// Blocks for one datagram and preserves source, flags, and original wire size.
[[nodiscard]] inline datagram_receive_result receive_datagram_blocking(
    int descriptor,
    std::span<std::byte> buffer) noexcept {
    return detail::receive_datagram_once(descriptor, buffer, 0, true);
}

/// Attempts one datagram receive without changing descriptor blocking mode.
[[nodiscard]] inline datagram_receive_result try_receive_datagram(
    int descriptor,
    std::span<std::byte> buffer) noexcept {
    return detail::receive_datagram_once(
        descriptor, buffer, MSG_DONTWAIT, true);
}

/// Spins with per-call MSG_DONTWAIT until a datagram or caller stop request.
template <typename StopRequested>
[[nodiscard]] datagram_receive_result receive_datagram_busy_spin(
    int descriptor,
    std::span<std::byte> buffer,
    StopRequested&& stop_requested) {
    lls::concurrency::busy_spin_wait spin_wait;
    while (true) {
        auto result = detail::receive_datagram_once(
            descriptor, buffer, MSG_DONTWAIT, false);
        if (result.state != datagram_receive_state::not_ready) {
            if (result.state == datagram_receive_state::error &&
                result.error ==
                    std::make_error_code(std::errc::interrupted)) {
                if (static_cast<bool>(stop_requested())) {
                    result.state = datagram_receive_state::stopped;
                    result.error.clear();
                    return result;
                }
                continue;
            }
            return result;
        }
        if (static_cast<bool>(stop_requested())) {
            result.state = datagram_receive_state::stopped;
            return result;
        }
        spin_wait.wait();
    }
}

/// Adapts a chrono deadline to the cooperative datagram stop predicate.
template <typename Clock, typename Duration>
[[nodiscard]] datagram_receive_result
receive_datagram_busy_spin_until(
    int descriptor,
    std::span<std::byte> buffer,
    std::chrono::time_point<Clock, Duration> deadline) {
    return receive_datagram_busy_spin(
        descriptor,
        buffer,
        [deadline] { return Clock::now() >= deadline; });
}

}  // namespace lls::networking
