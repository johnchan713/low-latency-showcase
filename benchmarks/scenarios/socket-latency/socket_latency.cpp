#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <lls/concurrency/spin_wait.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace {

constexpr std::uint16_t message_magic = 0x4c53;
constexpr std::uint8_t message_version = 1;
constexpr std::array<std::size_t, 4> payload_sizes{8, 64, 256, 1'400};
constexpr std::uint64_t nanoseconds_per_second = 1'000'000'000ULL;
constexpr std::uint64_t udp_warmup_timeout_ns =
    5ULL * nanoseconds_per_second;
constexpr std::uint64_t tcp_spin_safety_timeout_ns =
    5ULL * nanoseconds_per_second;
constexpr std::size_t spin_deadline_check_interval = 4'096;
constexpr int udp_server_poll_timeout_ms = 10;

enum class protocol : std::uint8_t { tcp = 1, udp = 2 };

enum class wait_strategy : std::uint8_t { kernel, spin };

enum class spin_relaxation : std::uint8_t {
    pause_every_miss,
    pause_every_four_misses,
    unpaused,
};

enum class udp_receive_api : std::uint8_t {
    recvmsg,
    connected_recvmsg,
    connected_recv,
};

struct profile_spec final {
    std::string_view name;
    bool supports_tcp;
    bool supports_udp;
    wait_strategy receive_wait;
    bool tcp_nodelay;
    bool tcp_quickack_rearm;
    bool udp_connected;
    int busy_poll_us;
    spin_relaxation relaxation;
    udp_receive_api udp_receive;
    std::size_t spin_control_check_interval{1};
};

constexpr std::array profiles{
    profile_spec{"baseline", true, true, wait_strategy::kernel, false,
                 false, false, 0, spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-nodelay", true, false, wait_strategy::kernel, true,
                 false, false, 0, spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-spin", true, false, wait_strategy::spin, false,
                 false, false, 0, spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-spin-pause4", true, false, wait_strategy::spin, false,
                 false, false, 0,
                 spin_relaxation::pause_every_four_misses,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-spin-unpaused", true, false, wait_strategy::spin,
                 false, false, false, 0, spin_relaxation::unpaused,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-nodelay-spin", true, false, wait_strategy::spin, true,
                 false, false, 0, spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-quickack", true, false, wait_strategy::kernel, false,
                 true, false, 0, spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-quickack-spin", true, false, wait_strategy::spin,
                 false, true, false, 0,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-nodelay-quickack", true, false,
                 wait_strategy::kernel, true, true, false, 0,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-nodelay-quickack-spin", true, false,
                 wait_strategy::spin, true, true, false, 0,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"udp-connected", false, true, wait_strategy::kernel, false,
                 false, true, 0, spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"udp-spin", false, true, wait_strategy::spin, false, false,
                 false, 0, spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"udp-connected-spin", false, true, wait_strategy::spin,
                 false, false, true, 0,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"udp-connected-spin-pause4", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::pause_every_four_misses,
                 udp_receive_api::recvmsg},
    profile_spec{"udp-connected-spin-unpaused", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::unpaused, udp_receive_api::recvmsg},
    profile_spec{"udp-connected-recv-spin", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::connected_recv},
    profile_spec{"udp-connected-peerless-recvmsg-spin", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::connected_recvmsg},
    profile_spec{"udp-connected-peerless-recvmsg-spin-pause4", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::pause_every_four_misses,
                 udp_receive_api::connected_recvmsg},
    profile_spec{"udp-connected-peerless-recvmsg-spin-unpaused", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::unpaused,
                 udp_receive_api::connected_recvmsg},
    profile_spec{"udp-connected-recv-spin-pause4", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::pause_every_four_misses,
                 udp_receive_api::connected_recv},
    profile_spec{"udp-connected-recv-spin-unpaused", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::unpaused,
                 udp_receive_api::connected_recv},
    profile_spec{"udp-connected-spin-check64", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg, 64},
    profile_spec{"udp-connected-recv-spin-check64", false, true,
                 wait_strategy::spin, false, false, true, 0,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::connected_recv, 64},
    profile_spec{"busy-poll-50", true, true, wait_strategy::kernel, false,
                 false, false, 50, spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"tcp-nodelay-busy-poll-50", true, false,
                 wait_strategy::kernel, true, false, false, 50,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
    profile_spec{"udp-connected-busy-poll-50", false, true,
                 wait_strategy::kernel, false, false, true, 50,
                 spin_relaxation::pause_every_miss,
                 udp_receive_api::recvmsg},
};

struct options final {
    std::size_t warmup{20'000};
    std::size_t samples{200'000};
    std::size_t runs{8};
    int client_cpu{-1};
    int server_cpu{-1};
    std::size_t udp_timeout_ms{100};
    const profile_spec* profile{&profiles.front()};
    std::optional<protocol> protocol_filter{};
    std::optional<std::size_t> payload_filter{};
};

class unique_fd final {
public:
    explicit unique_fd(int value = -1) noexcept : value_{value} {}
    ~unique_fd() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }

    unique_fd(const unique_fd&) = delete;
    unique_fd& operator=(const unique_fd&) = delete;
    unique_fd(unique_fd&& other) noexcept : value_{other.release()} {}
    unique_fd& operator=(unique_fd&& other) noexcept {
        if (this != &other) {
            if (value_ >= 0) {
                static_cast<void>(::close(value_));
            }
            value_ = other.release();
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept { return value_; }

private:
    [[nodiscard]] int release() noexcept {
        return std::exchange(value_, -1);
    }

    int value_;
};

[[noreturn]] void throw_errno(std::string_view operation) {
    throw std::system_error(errno, std::generic_category(),
                            std::string{operation});
}

[[nodiscard]] std::uint64_t clock_nanoseconds(clockid_t clock_id) {
    timespec value{};
    if (::clock_gettime(clock_id, &value) != 0) {
        throw_errno("clock_gettime");
    }
    if (value.tv_sec < 0 || value.tv_nsec < 0) {
        throw std::runtime_error("clock_gettime returned a negative value");
    }
    return static_cast<std::uint64_t>(value.tv_sec) * nanoseconds_per_second +
           static_cast<std::uint64_t>(value.tv_nsec);
}

[[nodiscard]] std::uint64_t timeval_nanoseconds(const timeval& value) {
    if (value.tv_sec < 0 || value.tv_usec < 0) {
        throw std::runtime_error("getrusage returned a negative value");
    }
    return static_cast<std::uint64_t>(value.tv_sec) *
               nanoseconds_per_second +
           static_cast<std::uint64_t>(value.tv_usec) * 1'000ULL;
}

struct usage_snapshot final {
    std::uint64_t thread_cpu_ns{};
    std::uint64_t user_ns{};
    std::uint64_t system_ns{};
    std::uint64_t voluntary_switches{};
    std::uint64_t involuntary_switches{};
};

[[nodiscard]] usage_snapshot current_thread_usage() {
    rusage usage{};
    if (::getrusage(RUSAGE_THREAD, &usage) != 0) {
        throw_errno("getrusage");
    }
    if (usage.ru_nvcsw < 0 || usage.ru_nivcsw < 0) {
        throw std::runtime_error("getrusage returned negative context switches");
    }
    return {
        clock_nanoseconds(CLOCK_THREAD_CPUTIME_ID),
        timeval_nanoseconds(usage.ru_utime),
        timeval_nanoseconds(usage.ru_stime),
        static_cast<std::uint64_t>(usage.ru_nvcsw),
        static_cast<std::uint64_t>(usage.ru_nivcsw),
    };
}

[[nodiscard]] usage_snapshot subtract(const usage_snapshot& finish,
                                      const usage_snapshot& start) {
    return {
        finish.thread_cpu_ns - start.thread_cpu_ns,
        finish.user_ns - start.user_ns,
        finish.system_ns - start.system_ns,
        finish.voluntary_switches - start.voluntary_switches,
        finish.involuntary_switches - start.involuntary_switches,
    };
}

void verify_current_affinity(int cpu) {
    cpu_set_t observed_set;
    CPU_ZERO(&observed_set);
    const auto result = ::pthread_getaffinity_np(
        ::pthread_self(), sizeof(observed_set), &observed_set);
    if (result != 0) {
        throw std::system_error(result, std::generic_category(),
                                "pthread_getaffinity_np");
    }
    if (CPU_COUNT(&observed_set) != 1 ||
        !CPU_ISSET(static_cast<std::size_t>(cpu), &observed_set) ||
        ::sched_getcpu() != cpu) {
        throw std::runtime_error("thread affinity verification failed");
    }
}

void pin_current_thread(int cpu) {
    if (cpu < 0 || cpu >= CPU_SETSIZE) {
        throw std::invalid_argument("CPU index is outside CPU_SETSIZE");
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<std::size_t>(cpu), &set);
    const auto result =
        ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
    if (result != 0) {
        throw std::system_error(result, std::generic_category(),
                                "pthread_setaffinity_np");
    }
    verify_current_affinity(cpu);
}

[[nodiscard]] std::vector<int> available_cpus() {
    cpu_set_t set;
    CPU_ZERO(&set);
    if (::sched_getaffinity(0, sizeof(set), &set) != 0) {
        throw_errno("sched_getaffinity");
    }
    std::vector<int> cpus;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(static_cast<std::size_t>(cpu), &set)) {
            cpus.push_back(cpu);
        }
    }
    return cpus;
}

void resolve_affinity(options& configuration) {
    if ((configuration.client_cpu < 0) != (configuration.server_cpu < 0)) {
        throw std::invalid_argument(
            "set both --client-cpu and --server-cpu, or neither");
    }
    if (configuration.client_cpu < 0) {
        const auto cpus = available_cpus();
        if (cpus.size() < 2) {
            throw std::runtime_error(
                "socket benchmark requires two available CPUs");
        }
        configuration.client_cpu = cpus[0];
        configuration.server_cpu = cpus[1];
    }
    if (configuration.client_cpu == configuration.server_cpu) {
        throw std::invalid_argument("client and server CPUs must differ");
    }
}

[[nodiscard]] unique_fd make_socket(int type, int socket_protocol) {
    const auto descriptor =
        ::socket(AF_INET, type | SOCK_CLOEXEC, socket_protocol);
    if (descriptor < 0) {
        throw_errno("socket");
    }
    return unique_fd{descriptor};
}

[[nodiscard]] sockaddr_in loopback_address(std::uint16_t port) noexcept {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return address;
}

[[nodiscard]] std::uint16_t bound_port(int descriptor) {
    sockaddr_in address{};
    socklen_t length = sizeof(address);
    if (::getsockname(descriptor,
                      reinterpret_cast<sockaddr*>(&address),
                      &length) != 0) {
        throw_errno("getsockname");
    }
    if (length != sizeof(address) || address.sin_family != AF_INET) {
        throw std::runtime_error("getsockname returned an unexpected address");
    }
    return ntohs(address.sin_port);
}

void bind_ephemeral_loopback(int descriptor) {
    const auto address = loopback_address(0);
    if (::bind(descriptor,
               reinterpret_cast<const sockaddr*>(&address),
               sizeof(address)) != 0) {
        throw_errno("bind");
    }
}

void send_all(int descriptor, std::span<const std::byte> bytes) {
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        const auto sent = ::send(descriptor,
                                 bytes.data() + offset,
                                 bytes.size() - offset,
                                 MSG_NOSIGNAL);
        if (sent > 0) {
            offset += static_cast<std::size_t>(sent);
        } else if (sent < 0 && errno == EINTR) {
            continue;
        } else if (sent == 0) {
            throw std::runtime_error("send made no progress");
        } else {
            throw_errno("send");
        }
    }
}

template <spin_relaxation Relaxation>
class socket_spin_wait final {
public:
    void wait() noexcept {
        if constexpr (Relaxation == spin_relaxation::unpaused) {
            return;
        }
        if constexpr (
            Relaxation == spin_relaxation::pause_every_four_misses) {
            ++misses_;
            if ((misses_ & 3U) != 0U) {
                return;
            }
        }
        pause_.wait();
    }

private:
    std::size_t misses_{};
    lls::concurrency::busy_spin_wait pause_{};
};

template <spin_relaxation Relaxation>
void receive_all_spinning(int descriptor,
                          std::span<std::byte> bytes,
                          std::uint64_t spin_deadline_ns) {
    std::size_t offset = 0;
    std::size_t spins_until_deadline_check = spin_deadline_check_interval;
    socket_spin_wait<Relaxation> spin_wait;
    while (offset < bytes.size()) {
        const auto received = ::recv(descriptor,
                                     bytes.data() + offset,
                                     bytes.size() - offset,
                                     MSG_DONTWAIT);
        if (received > 0) {
            offset += static_cast<std::size_t>(received);
        } else if (received < 0 && errno == EINTR) {
            continue;
        } else if (received < 0 &&
                   (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (spin_deadline_ns !=
                    std::numeric_limits<std::uint64_t>::max() &&
                --spins_until_deadline_check == 0) {
                if (clock_nanoseconds(CLOCK_MONOTONIC_RAW) >=
                    spin_deadline_ns) {
                    throw std::runtime_error(
                        "TCP spin receive exceeded its safety timeout");
                }
                spins_until_deadline_check = spin_deadline_check_interval;
            }
            spin_wait.wait();
        } else if (received == 0) {
            throw std::runtime_error("peer closed a partial message");
        } else {
            throw_errno("recv");
        }
    }
}

void receive_all(int descriptor,
                 std::span<std::byte> bytes,
                 wait_strategy strategy,
                 spin_relaxation relaxation,
                 std::uint64_t spin_deadline_ns =
                     std::numeric_limits<std::uint64_t>::max()) {
    if (strategy == wait_strategy::spin) {
        switch (relaxation) {
        case spin_relaxation::pause_every_miss:
            receive_all_spinning<spin_relaxation::pause_every_miss>(
                descriptor, bytes, spin_deadline_ns);
            return;
        case spin_relaxation::pause_every_four_misses:
            receive_all_spinning<
                spin_relaxation::pause_every_four_misses>(
                descriptor, bytes, spin_deadline_ns);
            return;
        case spin_relaxation::unpaused:
            receive_all_spinning<spin_relaxation::unpaused>(
                descriptor, bytes, spin_deadline_ns);
            return;
        }
        throw std::logic_error("unknown spin relaxation");
    }

    std::size_t offset = 0;
    while (offset < bytes.size()) {
        const auto received = ::recv(descriptor,
                                     bytes.data() + offset,
                                     bytes.size() - offset,
                                     0);
        if (received > 0) {
            offset += static_cast<std::size_t>(received);
        } else if (received < 0 && errno == EINTR) {
            continue;
        } else if (received == 0) {
            throw std::runtime_error("peer closed a partial message");
        } else {
            throw_errno("recv");
        }
    }
}

void send_connected_datagram(int descriptor,
                             std::span<const std::byte> bytes) {
    ssize_t sent{};
    do {
        sent = ::send(descriptor,
                      bytes.data(),
                      bytes.size(),
                      MSG_NOSIGNAL);
    } while (sent < 0 && errno == EINTR);
    if (sent < 0) {
        throw_errno("send");
    }
    if (static_cast<std::size_t>(sent) != bytes.size()) {
        throw std::runtime_error("partial connected UDP datagram");
    }
}

void send_datagram(int descriptor,
                   std::span<const std::byte> bytes,
                   const sockaddr_in& destination) {
    ssize_t sent{};
    do {
        sent = ::sendto(descriptor,
                        bytes.data(),
                        bytes.size(),
                        MSG_NOSIGNAL,
                        reinterpret_cast<const sockaddr*>(&destination),
                        sizeof(destination));
    } while (sent < 0 && errno == EINTR);
    if (sent < 0) {
        throw_errno("sendto");
    }
    if (static_cast<std::size_t>(sent) != bytes.size()) {
        throw std::runtime_error("partial UDP datagram");
    }
}

struct received_datagram final {
    ssize_t bytes{};
    sockaddr_in peer{};
    int flags{};
};

[[nodiscard]] received_datagram receive_datagram(
    int descriptor,
    std::span<std::byte> buffer) {
    received_datagram result{};
    iovec vector{buffer.data(), buffer.size()};
    msghdr message{};
    message.msg_name = &result.peer;
    message.msg_namelen = sizeof(result.peer);
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    do {
        result.bytes = ::recvmsg(descriptor, &message, MSG_TRUNC);
    } while (result.bytes < 0 && errno == EINTR);
    if (result.bytes < 0) {
        throw_errno("recvmsg");
    }
    result.flags = message.msg_flags;
    return result;
}

[[nodiscard]] std::optional<received_datagram> try_receive_datagram(
    int descriptor,
    std::span<std::byte> buffer) {
    received_datagram result{};
    iovec vector{buffer.data(), buffer.size()};
    msghdr message{};
    message.msg_name = &result.peer;
    message.msg_namelen = sizeof(result.peer);
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    do {
        result.bytes =
            ::recvmsg(descriptor, &message, MSG_TRUNC | MSG_DONTWAIT);
    } while (result.bytes < 0 && errno == EINTR);
    if (result.bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return std::nullopt;
    }
    if (result.bytes < 0) {
        throw_errno("recvmsg");
    }
    result.flags = message.msg_flags;
    return result;
}

[[nodiscard]] received_datagram receive_connected_datagram_message(
    int descriptor,
    std::span<std::byte> buffer) {
    received_datagram result{};
    iovec vector{buffer.data(), buffer.size()};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    do {
        result.bytes = ::recvmsg(descriptor, &message, MSG_TRUNC);
    } while (result.bytes < 0 && errno == EINTR);
    if (result.bytes < 0) {
        throw_errno("recvmsg connected datagram");
    }
    result.flags = message.msg_flags;
    return result;
}

[[nodiscard]] std::optional<received_datagram>
try_receive_connected_datagram_message(
    int descriptor,
    std::span<std::byte> buffer) {
    received_datagram result{};
    iovec vector{buffer.data(), buffer.size()};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    do {
        result.bytes =
            ::recvmsg(descriptor, &message, MSG_TRUNC | MSG_DONTWAIT);
    } while (result.bytes < 0 && errno == EINTR);
    if (result.bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return std::nullopt;
    }
    if (result.bytes < 0) {
        throw_errno("recvmsg connected datagram");
    }
    result.flags = message.msg_flags;
    return result;
}

[[nodiscard]] received_datagram receive_connected_datagram(
    int descriptor,
    std::span<std::byte> buffer) {
    received_datagram result{};
    do {
        result.bytes = ::recv(
            descriptor, buffer.data(), buffer.size(), MSG_TRUNC);
    } while (result.bytes < 0 && errno == EINTR);
    if (result.bytes < 0) {
        throw_errno("recv connected datagram");
    }
    if (result.bytes > static_cast<ssize_t>(buffer.size())) {
        result.flags = MSG_TRUNC;
    }
    return result;
}

[[nodiscard]] std::optional<received_datagram>
try_receive_connected_datagram(int descriptor,
                               std::span<std::byte> buffer) {
    received_datagram result{};
    do {
        result.bytes = ::recv(descriptor,
                              buffer.data(),
                              buffer.size(),
                              MSG_TRUNC | MSG_DONTWAIT);
    } while (result.bytes < 0 && errno == EINTR);
    if (result.bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return std::nullopt;
    }
    if (result.bytes < 0) {
        throw_errno("recv connected datagram");
    }
    if (result.bytes > static_cast<ssize_t>(buffer.size())) {
        result.flags = MSG_TRUNC;
    }
    return result;
}

template <udp_receive_api ReceiveApi>
[[nodiscard]] received_datagram receive_profile_datagram(
    int descriptor,
    std::span<std::byte> buffer) {
    if constexpr (ReceiveApi == udp_receive_api::connected_recv) {
        return receive_connected_datagram(descriptor, buffer);
    }
    if constexpr (ReceiveApi == udp_receive_api::connected_recvmsg) {
        return receive_connected_datagram_message(descriptor, buffer);
    }
    return receive_datagram(descriptor, buffer);
}

template <udp_receive_api ReceiveApi>
[[nodiscard]] std::optional<received_datagram>
try_receive_profile_datagram(int descriptor,
                             std::span<std::byte> buffer) {
    if constexpr (ReceiveApi == udp_receive_api::connected_recv) {
        return try_receive_connected_datagram(descriptor, buffer);
    }
    if constexpr (ReceiveApi == udp_receive_api::connected_recvmsg) {
        return try_receive_connected_datagram_message(descriptor, buffer);
    }
    return try_receive_datagram(descriptor, buffer);
}

[[nodiscard]] bool wait_readable_until(int descriptor,
                                       std::uint64_t deadline_ns) {
    while (true) {
        const auto now = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
        if (now >= deadline_ns) {
            return false;
        }
        const auto remaining_ns = deadline_ns - now;
        const auto rounded_ms =
            (remaining_ns + 999'999ULL) / 1'000'000ULL;
        const auto timeout_ms = static_cast<int>(std::min<std::uint64_t>(
            rounded_ms,
            static_cast<std::uint64_t>(std::numeric_limits<int>::max())));
        pollfd event{descriptor, POLLIN, 0};
        const auto ready = ::poll(&event, 1, timeout_ms);
        if (ready > 0) {
            if ((event.revents & POLLIN) != 0) {
                return true;
            }
            throw std::runtime_error("poll reported a UDP socket error");
        }
        if (ready == 0) {
            return false;
        }
        if (errno != EINTR) {
            throw_errno("poll");
        }
    }
}

[[nodiscard]] bool wait_for_udp_server_input(
    int descriptor,
    const std::atomic<bool>& cancellation_requested) {
    while (!cancellation_requested.load(std::memory_order_acquire)) {
        pollfd event{descriptor, POLLIN, 0};
        const auto ready =
            ::poll(&event, 1, udp_server_poll_timeout_ms);
        if (ready > 0) {
            if ((event.revents & POLLIN) != 0) {
                return true;
            }
            throw std::runtime_error("poll reported a UDP server socket error");
        }
        if (ready < 0 && errno != EINTR) {
            throw_errno("poll");
        }
    }
    return false;
}

template <udp_receive_api ReceiveApi, spin_relaxation Relaxation>
[[nodiscard]] std::optional<received_datagram>
receive_udp_until_spinning(
    int descriptor,
    std::span<std::byte> buffer,
    std::uint64_t deadline_ns,
    std::size_t control_check_interval) {
    if (clock_nanoseconds(CLOCK_MONOTONIC_RAW) >= deadline_ns) {
        return std::nullopt;
    }
    std::size_t misses_until_control_check = control_check_interval;
    socket_spin_wait<Relaxation> spin_wait;
    while (true) {
        auto datagram =
            try_receive_profile_datagram<ReceiveApi>(descriptor, buffer);
        if (datagram) {
            return datagram;
        }
        --misses_until_control_check;
        if (misses_until_control_check == 0) {
            if (clock_nanoseconds(CLOCK_MONOTONIC_RAW) >= deadline_ns) {
                return std::nullopt;
            }
            misses_until_control_check = control_check_interval;
        }
        spin_wait.wait();
    }
}

template <udp_receive_api ReceiveApi>
[[nodiscard]] std::optional<received_datagram>
receive_udp_until_with_api(int descriptor,
                           std::span<std::byte> buffer,
                           std::uint64_t deadline_ns,
                           const profile_spec& profile) {
    switch (profile.relaxation) {
    case spin_relaxation::pause_every_miss:
        return receive_udp_until_spinning<
            ReceiveApi, spin_relaxation::pause_every_miss>(
            descriptor,
            buffer,
            deadline_ns,
            profile.spin_control_check_interval);
    case spin_relaxation::pause_every_four_misses:
        return receive_udp_until_spinning<
            ReceiveApi, spin_relaxation::pause_every_four_misses>(
            descriptor,
            buffer,
            deadline_ns,
            profile.spin_control_check_interval);
    case spin_relaxation::unpaused:
        return receive_udp_until_spinning<
            ReceiveApi, spin_relaxation::unpaused>(
            descriptor,
            buffer,
            deadline_ns,
            profile.spin_control_check_interval);
    }
    throw std::logic_error("unknown spin relaxation");
}

[[nodiscard]] std::optional<received_datagram> receive_udp_until(
    int descriptor,
    std::span<std::byte> buffer,
    std::uint64_t deadline_ns,
    const profile_spec& profile) {
    if (profile.receive_wait == wait_strategy::kernel) {
        if (!wait_readable_until(descriptor, deadline_ns)) {
            return std::nullopt;
        }
        if (profile.udp_receive == udp_receive_api::connected_recv) {
            return receive_connected_datagram(descriptor, buffer);
        }
        if (profile.udp_receive == udp_receive_api::connected_recvmsg) {
            return receive_connected_datagram_message(descriptor, buffer);
        }
        return receive_datagram(descriptor, buffer);
    }
    if (profile.udp_receive == udp_receive_api::connected_recv) {
        return receive_udp_until_with_api<udp_receive_api::connected_recv>(
            descriptor, buffer, deadline_ns, profile);
    }
    if (profile.udp_receive == udp_receive_api::connected_recvmsg) {
        return receive_udp_until_with_api<
            udp_receive_api::connected_recvmsg>(
            descriptor, buffer, deadline_ns, profile);
    }
    return receive_udp_until_with_api<udp_receive_api::recvmsg>(
        descriptor, buffer, deadline_ns, profile);
}

template <udp_receive_api ReceiveApi, spin_relaxation Relaxation>
[[nodiscard]] std::optional<received_datagram>
receive_udp_server_spinning(
    int descriptor,
    std::span<std::byte> buffer,
    std::size_t control_check_interval,
    const std::atomic<bool>& cancellation_requested) {
    if (cancellation_requested.load(std::memory_order_acquire)) {
        return std::nullopt;
    }
    std::size_t misses_until_control_check = control_check_interval;
    socket_spin_wait<Relaxation> spin_wait;
    while (true) {
        auto datagram =
            try_receive_profile_datagram<ReceiveApi>(descriptor, buffer);
        if (datagram) {
            return datagram;
        }
        --misses_until_control_check;
        if (misses_until_control_check == 0) {
            if (cancellation_requested.load(std::memory_order_acquire)) {
                return std::nullopt;
            }
            misses_until_control_check = control_check_interval;
        }
        spin_wait.wait();
    }
}

template <udp_receive_api ReceiveApi>
[[nodiscard]] std::optional<received_datagram>
receive_udp_server_with_api(
    int descriptor,
    std::span<std::byte> buffer,
    const profile_spec& profile,
    const std::atomic<bool>& cancellation_requested) {
    switch (profile.relaxation) {
    case spin_relaxation::pause_every_miss:
        return receive_udp_server_spinning<
            ReceiveApi, spin_relaxation::pause_every_miss>(
            descriptor,
            buffer,
            profile.spin_control_check_interval,
            cancellation_requested);
    case spin_relaxation::pause_every_four_misses:
        return receive_udp_server_spinning<
            ReceiveApi, spin_relaxation::pause_every_four_misses>(
            descriptor,
            buffer,
            profile.spin_control_check_interval,
            cancellation_requested);
    case spin_relaxation::unpaused:
        return receive_udp_server_spinning<
            ReceiveApi, spin_relaxation::unpaused>(
            descriptor,
            buffer,
            profile.spin_control_check_interval,
            cancellation_requested);
    }
    throw std::logic_error("unknown spin relaxation");
}

[[nodiscard]] std::optional<received_datagram>
receive_udp_server_datagram(
    int descriptor,
    std::span<std::byte> buffer,
    const profile_spec& profile,
    const std::atomic<bool>& cancellation_requested) {
    if (profile.receive_wait == wait_strategy::kernel) {
        if (!wait_for_udp_server_input(descriptor,
                                       cancellation_requested)) {
            return std::nullopt;
        }
        if (profile.udp_receive == udp_receive_api::connected_recv) {
            return receive_connected_datagram(descriptor, buffer);
        }
        if (profile.udp_receive == udp_receive_api::connected_recvmsg) {
            return receive_connected_datagram_message(descriptor, buffer);
        }
        return receive_datagram(descriptor, buffer);
    }
    if (profile.udp_receive == udp_receive_api::connected_recv) {
        return receive_udp_server_with_api<
            udp_receive_api::connected_recv>(
            descriptor, buffer, profile, cancellation_requested);
    }
    if (profile.udp_receive == udp_receive_api::connected_recvmsg) {
        return receive_udp_server_with_api<
            udp_receive_api::connected_recvmsg>(
            descriptor, buffer, profile, cancellation_requested);
    }
    return receive_udp_server_with_api<udp_receive_api::recvmsg>(
        descriptor, buffer, profile, cancellation_requested);
}

void write_header(std::span<std::byte> bytes,
                  protocol selected_protocol,
                  std::uint32_t sequence) {
    if (bytes.size() < 8) {
        throw std::invalid_argument("wire message is smaller than its header");
    }
    const auto network_magic = htons(message_magic);
    const auto network_sequence = htonl(sequence);
    std::memcpy(bytes.data(), &network_magic, sizeof(network_magic));
    bytes[2] = static_cast<std::byte>(message_version);
    bytes[3] = static_cast<std::byte>(selected_protocol);
    std::memcpy(bytes.data() + 4, &network_sequence, sizeof(network_sequence));
}

[[nodiscard]] bool same_endpoint(const sockaddr_in& left,
                                 const sockaddr_in& right) noexcept {
    return left.sin_family == AF_INET && right.sin_family == AF_INET &&
           left.sin_port == right.sin_port &&
           left.sin_addr.s_addr == right.sin_addr.s_addr;
}

void initialize_message(std::span<std::byte> bytes,
                        protocol selected_protocol) {
    write_header(bytes, selected_protocol, 0);
    for (std::size_t index = 8; index < bytes.size(); ++index) {
        const auto mixed = static_cast<std::uint64_t>(index) * 17ULL + 0xa5ULL;
        bytes[index] = static_cast<std::byte>(mixed & 0xffULL);
    }
}

[[nodiscard]] std::uint32_t read_sequence(
    std::span<const std::byte> bytes,
    protocol expected_protocol) {
    if (bytes.size() < 8) {
        throw std::runtime_error("received message is smaller than its header");
    }
    std::uint16_t network_magic{};
    std::uint32_t network_sequence{};
    std::memcpy(&network_magic, bytes.data(), sizeof(network_magic));
    std::memcpy(&network_sequence, bytes.data() + 4, sizeof(network_sequence));
    if (ntohs(network_magic) != message_magic ||
        std::to_integer<std::uint8_t>(bytes[2]) != message_version ||
        std::to_integer<std::uint8_t>(bytes[3]) !=
            static_cast<std::uint8_t>(expected_protocol)) {
        throw std::runtime_error("received message header is invalid");
    }
    return ntohl(network_sequence);
}

[[nodiscard]] std::uint64_t update_checksum(
    std::uint64_t checksum,
    std::uint32_t sequence,
    std::size_t payload_bytes,
    protocol selected_protocol) noexcept {
    checksum ^= static_cast<std::uint64_t>(sequence) +
                static_cast<std::uint64_t>(payload_bytes) * 257ULL +
                static_cast<std::uint8_t>(selected_protocol);
    return checksum * 1'099'511'628'211ULL;
}

[[nodiscard]] int socket_integer_option(int descriptor,
                                        int level,
                                        int option_name) {
    int value{};
    socklen_t length = sizeof(value);
    if (::getsockopt(descriptor, level, option_name, &value, &length) != 0) {
        throw_errno("getsockopt");
    }
    if (length != sizeof(value)) {
        throw std::runtime_error("getsockopt returned an unexpected length");
    }
    return value;
}

void set_socket_integer_option(int descriptor,
                               int level,
                               int option_name,
                               int value) {
    if (::setsockopt(descriptor,
                     level,
                     option_name,
                     &value,
                     sizeof(value)) != 0) {
        throw_errno("setsockopt");
    }
}

[[nodiscard]] int configure_busy_poll(int descriptor,
                                      const profile_spec& profile) {
    if (profile.busy_poll_us > 0) {
        set_socket_integer_option(
            descriptor, SOL_SOCKET, SO_BUSY_POLL, profile.busy_poll_us);
    }
    const auto observed =
        socket_integer_option(descriptor, SOL_SOCKET, SO_BUSY_POLL);
    if (profile.busy_poll_us > 0 && observed != profile.busy_poll_us) {
        throw std::runtime_error("SO_BUSY_POLL verification failed");
    }
    return observed;
}

void rearm_tcp_quickack(int descriptor, const profile_spec& profile) {
    if (profile.tcp_quickack_rearm) {
        set_socket_integer_option(descriptor, IPPROTO_TCP, TCP_QUICKACK, 1);
    }
}

void send_profile_datagram(int descriptor,
                           std::span<const std::byte> bytes,
                           const sockaddr_in& destination,
                           const profile_spec& profile) {
    if (profile.udp_connected) {
        send_connected_datagram(descriptor, bytes);
        return;
    }
    send_datagram(descriptor, bytes, destination);
}

struct server_report final {
    usage_snapshot usage{};
    std::uint64_t messages{};
    std::uint64_t duplicates{};
    std::uint64_t reordered{};
    std::uint64_t measured_unique_requests{};
    std::uint64_t invalid{};
    std::uint64_t truncated{};
    int send_buffer_bytes{};
    int receive_buffer_bytes{};
    int tcp_nodelay{-1};
    int busy_poll_us{};
};

struct run_result final {
    protocol selected_protocol{};
    std::string_view profile_name{};
    std::string_view receive_wait{};
    std::size_t spin_pause_interval{};
    std::size_t udp_spin_control_check_interval{};
    std::string_view udp_receive_api_name{};
    bool udp_connected{};
    bool tcp_nodelay_requested{};
    bool tcp_quickack_rearm{};
    int busy_poll_requested_us{};
    int client_busy_poll_us{};
    int server_busy_poll_us{};
    std::size_t payload_bytes{};
    std::size_t run{};
    std::size_t samples{};
    std::size_t attempted_samples{};
    std::size_t warmup{};
    std::size_t udp_deadline_ms{};
    std::uint64_t minimum_ns{};
    std::uint64_t p50_ns{};
    std::uint64_t p90_ns{};
    std::uint64_t p95_ns{};
    std::uint64_t p99_ns{};
    std::uint64_t p999_ns{};
    std::uint64_t maximum_ns{};
    double round_trips_per_second{};
    double application_messages_per_second{};
    double client_cpu_percent{};
    double server_cpu_percent{};
    double process_cpu_percent{};
    double client_cpu_ns_per_attempt{};
    double server_cpu_ns_per_attempt{};
    std::uint64_t client_voluntary_switches{};
    std::uint64_t client_involuntary_switches{};
    std::uint64_t server_voluntary_switches{};
    std::uint64_t server_involuntary_switches{};
    std::uint64_t connect_ns{};
    int client_send_buffer_bytes{};
    int client_receive_buffer_bytes{};
    int server_send_buffer_bytes{};
    int server_receive_buffer_bytes{};
    int client_tcp_nodelay{};
    int server_tcp_nodelay{};
    std::uint64_t udp_deadline_misses{};
    std::uint64_t udp_requests_unobserved{};
    std::uint64_t udp_response_deadline_misses{};
    std::uint64_t udp_combined_duplicate_events{};
    std::uint64_t udp_combined_reordered_events{};
    std::uint64_t udp_combined_invalid_events{};
    std::uint64_t udp_combined_truncated_events{};
    std::uint64_t checksum{};
    std::size_t workload_position{};
};

[[nodiscard]] std::size_t spin_pause_interval(
    const profile_spec& profile) noexcept {
    if (profile.receive_wait != wait_strategy::spin) {
        return 0;
    }
    switch (profile.relaxation) {
    case spin_relaxation::pause_every_miss:
        return 1;
    case spin_relaxation::pause_every_four_misses:
        return 4;
    case spin_relaxation::unpaused:
        return 0;
    }
    return 0;
}

[[nodiscard]] std::string_view udp_receive_api_name(
    protocol selected_protocol,
    const profile_spec& profile) noexcept {
    if (selected_protocol != protocol::udp) {
        return "none";
    }
    switch (profile.udp_receive) {
    case udp_receive_api::recvmsg:
        return "recvmsg";
    case udp_receive_api::connected_recvmsg:
        return "recvmsg-no-peer";
    case udp_receive_api::connected_recv:
        return "recv";
    }
    return "unknown";
}

[[nodiscard]] std::uint64_t percentile(
    const std::vector<std::uint64_t>& sorted,
    std::size_t numerator,
    std::size_t denominator) {
    if (denominator == 0 || numerator > denominator) {
        throw std::invalid_argument("invalid percentile fraction");
    }
    const auto index = (sorted.size() - 1) * numerator / denominator;
    return sorted[index];
}

[[nodiscard]] run_result summarize(protocol selected_protocol,
                                   std::size_t payload_bytes,
                                   std::size_t run,
                                   const options& configuration,
                                   std::vector<std::uint64_t> samples,
                                   std::uint64_t wall_ns,
                                   const usage_snapshot& client_usage,
                                   const usage_snapshot& server_usage,
                                   std::uint64_t process_cpu_ns,
                                   std::uint64_t connect_ns,
                                   int client_send_buffer_bytes,
                                   int client_receive_buffer_bytes,
                                   int client_tcp_nodelay,
                                   int server_send_buffer_bytes,
                                   int server_receive_buffer_bytes,
                                   int server_tcp_nodelay,
                                   int client_busy_poll_us,
                                   int server_busy_poll_us,
                                   std::uint64_t udp_deadline_misses,
                                   std::uint64_t udp_requests_unobserved,
                                   std::uint64_t udp_response_deadline_misses,
                                   std::uint64_t udp_combined_duplicate_events,
                                   std::uint64_t udp_combined_reordered_events,
                                   std::uint64_t udp_combined_invalid_events,
                                   std::uint64_t udp_combined_truncated_events,
                                   std::uint64_t checksum) {
    if (samples.empty()) {
        throw std::runtime_error("run completed without a latency sample");
    }
    std::sort(samples.begin(), samples.end());
    const auto successful_count = static_cast<double>(samples.size());
    const auto attempted_count = static_cast<double>(configuration.samples);
    const auto wall = static_cast<double>(wall_ns);
    const auto rate = successful_count *
                      static_cast<double>(nanoseconds_per_second) / wall;
    return {
        selected_protocol,
        configuration.profile->name,
        configuration.profile->receive_wait == wait_strategy::spin
            ? std::string_view{"spin"}
            : selected_protocol == protocol::tcp
                  ? std::string_view{"blocking"}
                  : std::string_view{"poll"},
        spin_pause_interval(*configuration.profile),
        selected_protocol == protocol::udp &&
                configuration.profile->receive_wait == wait_strategy::spin
            ? configuration.profile->spin_control_check_interval
            : 0,
        udp_receive_api_name(selected_protocol, *configuration.profile),
        configuration.profile->udp_connected,
        configuration.profile->tcp_nodelay,
        configuration.profile->tcp_quickack_rearm,
        configuration.profile->busy_poll_us,
        client_busy_poll_us,
        server_busy_poll_us,
        payload_bytes,
        run,
        samples.size(),
        configuration.samples,
        configuration.warmup,
        configuration.udp_timeout_ms,
        samples.front(),
        percentile(samples, 50, 100),
        percentile(samples, 90, 100),
        percentile(samples, 95, 100),
        percentile(samples, 99, 100),
        percentile(samples, 999, 1'000),
        samples.back(),
        rate,
        rate * 2.0,
        static_cast<double>(client_usage.thread_cpu_ns) / wall * 100.0,
        static_cast<double>(server_usage.thread_cpu_ns) / wall * 100.0,
        static_cast<double>(process_cpu_ns) / wall * 100.0,
        static_cast<double>(client_usage.thread_cpu_ns) / attempted_count,
        static_cast<double>(server_usage.thread_cpu_ns) / attempted_count,
        client_usage.voluntary_switches,
        client_usage.involuntary_switches,
        server_usage.voluntary_switches,
        server_usage.involuntary_switches,
        connect_ns,
        client_send_buffer_bytes,
        client_receive_buffer_bytes,
        server_send_buffer_bytes,
        server_receive_buffer_bytes,
        client_tcp_nodelay,
        server_tcp_nodelay,
        udp_deadline_misses,
        udp_requests_unobserved,
        udp_response_deadline_misses,
        udp_combined_duplicate_events,
        udp_combined_reordered_events,
        udp_combined_invalid_events,
        udp_combined_truncated_events,
        checksum,
    };
}

template <typename Exchange, typename PostExchange>
[[nodiscard]] run_result run_client(protocol selected_protocol,
                                    std::size_t payload_bytes,
                                    std::size_t run,
                                    const options& configuration,
                                    Exchange&& exchange,
                                    PostExchange&& post_exchange,
                                    server_report& server,
                                    std::thread& server_thread,
                                    std::exception_ptr& server_error,
                                    std::uint64_t connect_ns,
                                    int client_send_buffer_bytes,
                                    int client_receive_buffer_bytes,
                                    int client_tcp_nodelay,
                                    int client_busy_poll_us) {
    std::vector<std::byte> outbound(payload_bytes);
    std::vector<std::byte> inbound(payload_bytes);
    std::vector<std::uint64_t> latencies(configuration.samples);
    initialize_message(outbound, selected_protocol);
    std::uint64_t checksum = 1'469'598'103'934'665'603ULL;
    std::uint64_t expected_checksum = checksum;
    std::uint64_t valid_responses = 0;
    std::uint64_t duplicates = 0;
    std::uint64_t reordered = 0;
    const auto total = configuration.warmup + configuration.samples;
    if (total > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("warmup plus samples exceeds wire sequence");
    }
    for (std::size_t index = 0; index < configuration.samples; ++index) {
        const auto sequence = static_cast<std::uint32_t>(
            configuration.warmup + index);
        expected_checksum = update_checksum(
            expected_checksum, sequence, payload_bytes, selected_protocol);
    }

    for (std::size_t index = 0; index < configuration.warmup; ++index) {
        const auto sequence = static_cast<std::uint32_t>(index);
        write_header(outbound, selected_protocol, sequence);
        const auto exchange_started =
            clock_nanoseconds(CLOCK_MONOTONIC_RAW);
        exchange(outbound, inbound, exchange_started);
        post_exchange();
        if (inbound != outbound ||
            read_sequence(inbound, selected_protocol) != sequence) {
            throw std::runtime_error("warmup payload validation failed");
        }
    }

    verify_current_affinity(configuration.client_cpu);
    const auto client_start = current_thread_usage();
    const auto process_start = clock_nanoseconds(CLOCK_PROCESS_CPUTIME_ID);
    const auto wall_start = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
    for (std::size_t index = 0; index < configuration.samples; ++index) {
        const auto absolute_index = configuration.warmup + index;
        const auto sequence = static_cast<std::uint32_t>(absolute_index);
        write_header(outbound, selected_protocol, sequence);
        const auto started = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
        exchange(outbound, inbound, started);
        const auto finished = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
        post_exchange();
        if (finished <= started) {
            throw std::runtime_error("non-positive RTT sample");
        }
        latencies[index] = finished - started;

        const auto observed = read_sequence(inbound, selected_protocol);
        if (observed < sequence) {
            ++duplicates;
            throw std::runtime_error("duplicate response in serialized workload");
        }
        if (observed > sequence) {
            ++reordered;
            throw std::runtime_error("reordered response in serialized workload");
        }
        if (inbound != outbound) {
            throw std::runtime_error("measured payload validation failed");
        }
        checksum = update_checksum(
            checksum, sequence, payload_bytes, selected_protocol);
        ++valid_responses;
    }
    const auto wall_finish = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
    const auto process_finish = clock_nanoseconds(CLOCK_PROCESS_CPUTIME_ID);
    const auto client_finish = current_thread_usage();
    verify_current_affinity(configuration.client_cpu);

    server_thread.join();
    if (server_error) {
        std::rethrow_exception(server_error);
    }
    if (server.messages != total) {
        throw std::runtime_error("server message count validation failed");
    }
    if (checksum != expected_checksum) {
        throw std::runtime_error("aggregate sequence checksum validation failed");
    }

    const auto deadline_misses =
        static_cast<std::uint64_t>(configuration.samples) - valid_responses;
    return summarize(selected_protocol,
                     payload_bytes,
                     run,
                     configuration,
                     std::move(latencies),
                     wall_finish - wall_start,
                     subtract(client_finish, client_start),
                     server.usage,
                     process_finish - process_start,
                     connect_ns,
                     client_send_buffer_bytes,
                     client_receive_buffer_bytes,
                     client_tcp_nodelay,
                     server.send_buffer_bytes,
                     server.receive_buffer_bytes,
                     server.tcp_nodelay,
                     client_busy_poll_us,
                     server.busy_poll_us,
                     deadline_misses,
                     0,
                     0,
                     duplicates + server.duplicates,
                     reordered + server.reordered,
                     0,
                     0,
                     checksum);
}

[[nodiscard]] run_result run_tcp(std::size_t payload_bytes,
                                 std::size_t run,
                                 const options& configuration) {
    const auto& profile = *configuration.profile;
    auto listener = make_socket(SOCK_STREAM, IPPROTO_TCP);
    bind_ephemeral_loopback(listener.get());
    if (::listen(listener.get(), 1) != 0) {
        throw_errno("listen");
    }
    const auto server_address = loopback_address(bound_port(listener.get()));

    auto client = make_socket(SOCK_STREAM, IPPROTO_TCP);
    if (profile.tcp_nodelay) {
        set_socket_integer_option(
            client.get(), IPPROTO_TCP, TCP_NODELAY, 1);
    }
    const auto connect_started = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
    if (::connect(client.get(),
                  reinterpret_cast<const sockaddr*>(&server_address),
                  sizeof(server_address)) != 0) {
        throw_errno("connect");
    }
    const auto connect_finished = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
    const auto client_busy_poll = configure_busy_poll(client.get(), profile);
    rearm_tcp_quickack(client.get(), profile);
    const auto client_send_buffer =
        socket_integer_option(client.get(), SOL_SOCKET, SO_SNDBUF);
    const auto client_receive_buffer =
        socket_integer_option(client.get(), SOL_SOCKET, SO_RCVBUF);
    const auto client_nodelay =
        socket_integer_option(client.get(), IPPROTO_TCP, TCP_NODELAY);
    if (client_nodelay != static_cast<int>(profile.tcp_nodelay)) {
        throw std::runtime_error("client TCP_NODELAY verification failed");
    }

    server_report server{};
    std::exception_ptr server_error;
    std::atomic<int> server_state{0};
    std::thread server_thread([&] {
        try {
            pin_current_thread(configuration.server_cpu);
            sockaddr_in peer{};
            socklen_t peer_length = sizeof(peer);
            unique_fd connection{::accept(
                listener.get(), reinterpret_cast<sockaddr*>(&peer), &peer_length)};
            if (connection.get() < 0) {
                throw_errno("accept");
            }
            if (profile.tcp_nodelay) {
                set_socket_integer_option(
                    connection.get(), IPPROTO_TCP, TCP_NODELAY, 1);
            }
            server.busy_poll_us =
                configure_busy_poll(connection.get(), profile);
            rearm_tcp_quickack(connection.get(), profile);
            server.send_buffer_bytes =
                socket_integer_option(connection.get(), SOL_SOCKET, SO_SNDBUF);
            server.receive_buffer_bytes =
                socket_integer_option(connection.get(), SOL_SOCKET, SO_RCVBUF);
            server.tcp_nodelay =
                socket_integer_option(connection.get(), IPPROTO_TCP, TCP_NODELAY);
            if (server.tcp_nodelay !=
                static_cast<int>(profile.tcp_nodelay)) {
                throw std::runtime_error(
                    "server TCP_NODELAY verification failed");
            }
            server_state.store(1, std::memory_order_release);
            std::vector<std::byte> message(payload_bytes);
            const auto total = configuration.warmup + configuration.samples;
            usage_snapshot started{};
            for (std::size_t index = 0; index < total; ++index) {
                if (index == configuration.warmup) {
                    started = current_thread_usage();
                }
                receive_all(
                    connection.get(),
                    message,
                    profile.receive_wait,
                    profile.relaxation);
                send_all(connection.get(), message);
                rearm_tcp_quickack(connection.get(), profile);
                ++server.messages;
            }
            const auto finished = current_thread_usage();
            verify_current_affinity(configuration.server_cpu);
            server.usage = subtract(finished, started);
        } catch (...) {
            server_error = std::current_exception();
            server_state.store(-1, std::memory_order_release);
        }
    });

    try {
        while (server_state.load(std::memory_order_acquire) == 0) {
            std::this_thread::yield();
        }
        if (server_state.load(std::memory_order_acquire) < 0) {
            server_thread.join();
            std::rethrow_exception(server_error);
        }
        auto exchange = [&client, &profile](
                            std::span<const std::byte> outbound,
                            std::span<std::byte> inbound,
                            std::uint64_t started) {
            send_all(client.get(), outbound);
            receive_all(client.get(),
                        inbound,
                        profile.receive_wait,
                        profile.relaxation,
                        started + tcp_spin_safety_timeout_ns);
        };
        auto post_exchange = [&client, &profile] {
            rearm_tcp_quickack(client.get(), profile);
        };
        return run_client(protocol::tcp,
                          payload_bytes,
                          run,
                          configuration,
                          exchange,
                          post_exchange,
                          server,
                          server_thread,
                          server_error,
                          connect_finished - connect_started,
                          client_send_buffer,
                          client_receive_buffer,
                          client_nodelay,
                          client_busy_poll);
    } catch (...) {
        if (server_thread.joinable()) {
            static_cast<void>(::shutdown(client.get(), SHUT_RDWR));
            server_thread.join();
        }
        throw;
    }
}

[[nodiscard]] run_result run_udp(std::size_t payload_bytes,
                                 std::size_t run,
                                 const options& configuration) {
    const auto& profile = *configuration.profile;
    auto server_socket = make_socket(SOCK_DGRAM, IPPROTO_UDP);
    bind_ephemeral_loopback(server_socket.get());
    const auto server_address =
        loopback_address(bound_port(server_socket.get()));
    auto client = make_socket(SOCK_DGRAM, IPPROTO_UDP);
    if (profile.udp_connected) {
        bind_ephemeral_loopback(client.get());
        const auto client_address =
            loopback_address(bound_port(client.get()));
        if (::connect(client.get(),
                      reinterpret_cast<const sockaddr*>(&server_address),
                      sizeof(server_address)) != 0) {
            throw_errno("connect UDP client");
        }
        if (::connect(server_socket.get(),
                      reinterpret_cast<const sockaddr*>(&client_address),
                      sizeof(client_address)) != 0) {
            throw_errno("connect UDP server");
        }
    }
    const auto client_busy_poll = configure_busy_poll(client.get(), profile);
    const auto client_send_buffer =
        socket_integer_option(client.get(), SOL_SOCKET, SO_SNDBUF);
    const auto client_receive_buffer =
        socket_integer_option(client.get(), SOL_SOCKET, SO_RCVBUF);

    const auto total = configuration.warmup + configuration.samples;
    server_report server{};
    server.send_buffer_bytes =
        socket_integer_option(server_socket.get(), SOL_SOCKET, SO_SNDBUF);
    server.receive_buffer_bytes =
        socket_integer_option(server_socket.get(), SOL_SOCKET, SO_RCVBUF);
    server.busy_poll_us = configure_busy_poll(server_socket.get(), profile);
    std::exception_ptr server_error;
    std::atomic<int> server_state{0};
    std::atomic<bool> cancellation_requested{false};
    std::thread server_thread([&] {
        try {
            pin_current_thread(configuration.server_cpu);
            std::vector<std::byte> message(payload_bytes);
            std::vector<std::uint8_t> measured_seen(configuration.samples);
            bool high_water_valid = false;
            std::uint32_t high_water = 0;
            bool usage_started = false;
            bool usage_finished = false;
            usage_snapshot started{};
            server_state.store(1, std::memory_order_release);
            while (true) {
                auto received = receive_udp_server_datagram(
                    server_socket.get(),
                    message,
                    profile,
                    cancellation_requested);
                if (!received) {
                    break;
                }
                const auto datagram = *received;
                if ((datagram.flags & MSG_TRUNC) != 0 ||
                    datagram.bytes > static_cast<ssize_t>(message.size())) {
                    ++server.truncated;
                    continue;
                }
                if (datagram.bytes != static_cast<ssize_t>(message.size())) {
                    ++server.invalid;
                    continue;
                }
                std::uint32_t observed{};
                try {
                    observed = read_sequence(message, protocol::udp);
                } catch (const std::exception&) {
                    ++server.invalid;
                    continue;
                }
                if (observed >= total) {
                    ++server.invalid;
                    continue;
                }
                send_profile_datagram(server_socket.get(),
                                      message,
                                      datagram.peer,
                                      profile);
                ++server.messages;

                if (observed < configuration.warmup) {
                    if (observed + 1 == configuration.warmup) {
                        started = current_thread_usage();
                        usage_started = true;
                    }
                    continue;
                }
                const auto measured_index = static_cast<std::size_t>(
                    observed - configuration.warmup);
                if (measured_seen[measured_index] != 0) {
                    ++server.duplicates;
                    continue;
                }
                measured_seen[measured_index] = 1;
                ++server.measured_unique_requests;
                if (high_water_valid && observed < high_water) {
                    ++server.reordered;
                }
                if (!high_water_valid || observed > high_water) {
                    high_water = observed;
                    high_water_valid = true;
                }
                if (observed + 1 == total) {
                    const auto finished = current_thread_usage();
                    verify_current_affinity(configuration.server_cpu);
                    server.usage = subtract(finished, started);
                    usage_finished = true;
                }
            }
            if (usage_started && !usage_finished) {
                const auto finished = current_thread_usage();
                verify_current_affinity(configuration.server_cpu);
                server.usage = subtract(finished, started);
            }
            server_state.store(2, std::memory_order_release);
        } catch (...) {
            server_error = std::current_exception();
            server_state.store(-1, std::memory_order_release);
        }
    });

    try {
        while (server_state.load(std::memory_order_acquire) == 0) {
            std::this_thread::yield();
        }
        if (server_state.load(std::memory_order_acquire) < 0) {
            server_thread.join();
            std::rethrow_exception(server_error);
        }

        const auto timeout_ns =
            static_cast<std::uint64_t>(configuration.udp_timeout_ms) *
            static_cast<std::uint64_t>(1'000'000);
        std::vector<std::byte> outbound(payload_bytes);
        std::vector<std::byte> inbound(payload_bytes);
        std::vector<std::byte> expected(payload_bytes);
        initialize_message(outbound, protocol::udp);
        initialize_message(expected, protocol::udp);
        const auto warmup_timeout_ns =
            std::max(timeout_ns, udp_warmup_timeout_ns);

        for (std::size_t index = 0; index < configuration.warmup; ++index) {
            const auto sequence = static_cast<std::uint32_t>(index);
            write_header(outbound, protocol::udp, sequence);
            const auto sent_at = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
            send_profile_datagram(
                client.get(), outbound, server_address, profile);
            auto received = receive_udp_until(client.get(),
                                              inbound,
                                              sent_at + warmup_timeout_ns,
                                              profile);
            if (!received) {
                throw std::runtime_error("UDP warmup response timed out");
            }
            const auto datagram = *received;
            if ((datagram.flags & MSG_TRUNC) != 0 ||
                datagram.bytes != static_cast<ssize_t>(inbound.size()) ||
                (!profile.udp_connected &&
                 !same_endpoint(datagram.peer, server_address)) ||
                read_sequence(inbound, protocol::udp) != sequence ||
                inbound != outbound) {
                throw std::runtime_error("UDP warmup validation failed");
            }
        }

        std::vector<std::uint64_t> latencies;
        latencies.reserve(configuration.samples);
        std::vector<std::uint8_t> response_seen(configuration.samples);
        std::uint64_t checksum = 1'469'598'103'934'665'603ULL;
        std::uint64_t duplicates = 0;
        std::uint64_t reordered = 0;
        std::uint64_t invalid = 0;
        std::uint64_t truncated = 0;
        bool high_water_valid = false;
        std::uint32_t high_water = 0;

        verify_current_affinity(configuration.client_cpu);
        const auto client_start = current_thread_usage();
        const auto process_start =
            clock_nanoseconds(CLOCK_PROCESS_CPUTIME_ID);
        const auto wall_start = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
        for (std::size_t index = 0; index < configuration.samples; ++index) {
            const auto absolute_index = configuration.warmup + index;
            const auto sequence = static_cast<std::uint32_t>(absolute_index);
            write_header(outbound, protocol::udp, sequence);
            const auto sent_at = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
            send_profile_datagram(
                client.get(), outbound, server_address, profile);
            const auto deadline = sent_at + timeout_ns;
            bool current_received = false;
            while (true) {
                auto received = receive_udp_until(client.get(),
                                                  inbound,
                                                  deadline,
                                                  profile);
                if (!received) {
                    break;
                }
                const auto datagram = *received;
                const auto received_at =
                    clock_nanoseconds(CLOCK_MONOTONIC_RAW);
                if ((datagram.flags & MSG_TRUNC) != 0 ||
                    datagram.bytes > static_cast<ssize_t>(inbound.size())) {
                    ++truncated;
                    continue;
                }
                if (datagram.bytes != static_cast<ssize_t>(inbound.size()) ||
                    (!profile.udp_connected &&
                     !same_endpoint(datagram.peer, server_address))) {
                    ++invalid;
                    continue;
                }
                std::uint32_t observed{};
                try {
                    observed = read_sequence(inbound, protocol::udp);
                } catch (const std::exception&) {
                    ++invalid;
                    continue;
                }
                if (observed < configuration.warmup || observed >= total) {
                    ++invalid;
                    continue;
                }
                write_header(expected, protocol::udp, observed);
                if (inbound != expected) {
                    ++invalid;
                    continue;
                }
                const auto response_index = static_cast<std::size_t>(
                    observed - configuration.warmup);
                if (response_seen[response_index] != 0) {
                    ++duplicates;
                    continue;
                }
                response_seen[response_index] = 1;
                if (high_water_valid && observed < high_water) {
                    ++reordered;
                }
                if (!high_water_valid || observed > high_water) {
                    high_water = observed;
                    high_water_valid = true;
                }
                if (observed != sequence || received_at > deadline) {
                    continue;
                }
                if (received_at <= sent_at) {
                    throw std::runtime_error("non-positive UDP RTT sample");
                }
                latencies.push_back(received_at - sent_at);
                checksum = update_checksum(
                    checksum, observed, payload_bytes, protocol::udp);
                current_received = true;
                break;
            }
            if (!current_received &&
                server_state.load(std::memory_order_acquire) < 0) {
                throw std::runtime_error("UDP server failed during measurement");
            }
        }
        const auto wall_finish = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
        const auto process_finish =
            clock_nanoseconds(CLOCK_PROCESS_CPUTIME_ID);
        const auto client_finish = current_thread_usage();
        verify_current_affinity(configuration.client_cpu);

        cancellation_requested.store(true, std::memory_order_release);
        server_thread.join();
        if (server_error) {
            std::rethrow_exception(server_error);
        }
        const auto completed = static_cast<std::uint64_t>(latencies.size());
        const auto attempted =
            static_cast<std::uint64_t>(configuration.samples);
        const auto requests_unobserved =
            attempted - server.measured_unique_requests;
        const auto deadline_misses = attempted - completed;
        if (requests_unobserved > deadline_misses) {
            throw std::runtime_error("inconsistent UDP deadline accounting");
        }
        const auto response_deadline_misses =
            deadline_misses - requests_unobserved;
        return summarize(protocol::udp,
                         payload_bytes,
                         run,
                         configuration,
                         std::move(latencies),
                         wall_finish - wall_start,
                         subtract(client_finish, client_start),
                         server.usage,
                         process_finish - process_start,
                         0,
                         client_send_buffer,
                         client_receive_buffer,
                         -1,
                         server.send_buffer_bytes,
                         server.receive_buffer_bytes,
                         -1,
                         client_busy_poll,
                         server.busy_poll_us,
                         deadline_misses,
                         requests_unobserved,
                         response_deadline_misses,
                         duplicates + server.duplicates,
                         reordered + server.reordered,
                         invalid + server.invalid,
                         truncated + server.truncated,
                         checksum);
    } catch (...) {
        cancellation_requested.store(true, std::memory_order_release);
        if (server_thread.joinable()) {
            server_thread.join();
        }
        throw;
    }
}

[[nodiscard]] std::uint64_t clock_overhead_p50() {
    constexpr std::size_t readings = 100'000;
    std::vector<std::uint64_t> samples(readings);
    for (auto& sample : samples) {
        const auto start = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
        const auto finish = clock_nanoseconds(CLOCK_MONOTONIC_RAW);
        sample = finish - start;
    }
    std::sort(samples.begin(), samples.end());
    return percentile(samples, 50, 100);
}

[[nodiscard]] std::string_view protocol_name(protocol value) noexcept {
    return value == protocol::tcp ? "tcp" : "udp";
}

void print_header() {
    std::cout
        << "profile,protocol,receive_wait,udp_connected,"
           "spin_pause_interval,udp_spin_control_check_interval,"
           "udp_receive_api,"
           "tcp_nodelay_requested,tcp_quickack_rearm,"
           "busy_poll_requested_us,client_busy_poll_us,server_busy_poll_us,"
           "payload_bytes,run,workload_position,successful_samples,"
           "attempted_samples,warmup,udp_deadline_ms,min_ns,p50_ns,p90_ns,"
           "p95_ns,p99_ns,p999_ns,max_ns,"
           "validated_round_trips_per_second,"
           "application_messages_per_second,client_cpu_percent,"
           "server_cpu_percent,process_cpu_percent,"
           "client_cpu_ns_per_attempt,server_cpu_ns_per_attempt,"
           "client_voluntary_context_switches,"
           "client_involuntary_context_switches,"
           "server_voluntary_context_switches,"
           "server_involuntary_context_switches,tcp_connect_ns,"
           "client_send_buffer_bytes,client_receive_buffer_bytes,"
           "server_send_buffer_bytes,server_receive_buffer_bytes,"
           "client_tcp_nodelay,server_tcp_nodelay,udp_deadline_misses,"
           "udp_requests_unobserved,udp_response_deadline_misses,"
           "udp_combined_duplicate_events,udp_combined_reordered_events,"
           "udp_combined_invalid_events,udp_combined_truncated_events,"
           "checksum,validation\n";
}

void print_result(const run_result& result) {
    std::cout << result.profile_name << ','
              << protocol_name(result.selected_protocol) << ','
              << result.receive_wait << ',' << result.udp_connected << ','
              << result.spin_pause_interval << ','
              << result.udp_spin_control_check_interval << ','
              << result.udp_receive_api_name << ','
              << result.tcp_nodelay_requested << ','
              << result.tcp_quickack_rearm << ','
              << result.busy_poll_requested_us << ','
              << result.client_busy_poll_us << ','
              << result.server_busy_poll_us << ','
              << result.payload_bytes << ',' << result.run << ','
              << result.workload_position << ',' << result.samples << ','
              << result.attempted_samples << ',' << result.warmup << ','
              << result.udp_deadline_ms << ',' << result.minimum_ns << ','
              << result.p50_ns << ','
              << result.p90_ns << ',' << result.p95_ns << ','
              << result.p99_ns << ',' << result.p999_ns << ','
              << result.maximum_ns << ',' << std::fixed
              << std::setprecision(3) << result.round_trips_per_second << ','
              << result.application_messages_per_second << ','
              << result.client_cpu_percent << ',' << result.server_cpu_percent
              << ',' << result.process_cpu_percent << ','
              << result.client_cpu_ns_per_attempt << ','
              << result.server_cpu_ns_per_attempt << ','
              << result.client_voluntary_switches << ','
              << result.client_involuntary_switches << ','
              << result.server_voluntary_switches << ','
              << result.server_involuntary_switches << ','
              << result.connect_ns << ',' << result.client_send_buffer_bytes
              << ',' << result.client_receive_buffer_bytes << ','
              << result.server_send_buffer_bytes << ','
              << result.server_receive_buffer_bytes << ','
              << result.client_tcp_nodelay << ','
              << result.server_tcp_nodelay << ','
              << result.udp_deadline_misses << ','
              << result.udp_requests_unobserved << ','
              << result.udp_response_deadline_misses << ','
              << result.udp_combined_duplicate_events << ','
              << result.udp_combined_reordered_events << ','
              << result.udp_combined_invalid_events << ','
              << result.udp_combined_truncated_events << ','
              << result.checksum << ",PASS\n";
}

[[nodiscard]] std::size_t parse_size(std::string_view text,
                                     std::string_view option_name) {
    std::uint64_t parsed{};
    const auto* first = text.data();
    const auto* last = first + text.size();
    const auto [end, error] = std::from_chars(first, last, parsed);
    if (error != std::errc{} || end != last || parsed == 0 ||
        parsed > std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument(std::string{option_name} +
                                    " requires a positive integer");
    }
    return static_cast<std::size_t>(parsed);
}

[[nodiscard]] int parse_cpu(std::string_view text,
                            std::string_view option_name) {
    int parsed{};
    const auto* first = text.data();
    const auto* last = first + text.size();
    const auto [end, error] = std::from_chars(first, last, parsed);
    if (error != std::errc{} || end != last || parsed < 0) {
        throw std::invalid_argument(std::string{option_name} +
                                    " requires a non-negative integer");
    }
    return parsed;
}

[[nodiscard]] const profile_spec& parse_profile(std::string_view name) {
    const auto found = std::find_if(
        profiles.begin(), profiles.end(), [name](const profile_spec& profile) {
            return profile.name == name;
        });
    if (found == profiles.end()) {
        throw std::invalid_argument("unknown socket profile: " +
                                    std::string{name});
    }
    return *found;
}

[[nodiscard]] protocol parse_protocol(std::string_view name) {
    if (name == "tcp") {
        return protocol::tcp;
    }
    if (name == "udp") {
        return protocol::udp;
    }
    throw std::invalid_argument("--protocol requires tcp or udp");
}

[[nodiscard]] std::size_t parse_payload(std::string_view text) {
    const auto parsed = parse_size(text, "--payload");
    if (std::find(payload_sizes.begin(), payload_sizes.end(), parsed) ==
        payload_sizes.end()) {
        throw std::invalid_argument(
            "--payload requires 8, 64, 256, or 1400");
    }
    return parsed;
}

[[nodiscard]] options parse_options(int argc, char** argv) {
    options configuration;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{argv[index]};
        const auto value = [&]() -> std::string_view {
            if (index + 1 >= argc) {
                throw std::invalid_argument(std::string{argument} +
                                            " requires a value");
            }
            ++index;
            return argv[index];
        };
        if (argument == "--warmup") {
            configuration.warmup = parse_size(value(), argument);
        } else if (argument == "--samples") {
            configuration.samples = parse_size(value(), argument);
        } else if (argument == "--runs") {
            configuration.runs = parse_size(value(), argument);
        } else if (argument == "--client-cpu") {
            configuration.client_cpu = parse_cpu(value(), argument);
        } else if (argument == "--server-cpu") {
            configuration.server_cpu = parse_cpu(value(), argument);
        } else if (argument == "--udp-timeout-ms") {
            configuration.udp_timeout_ms = parse_size(value(), argument);
        } else if (argument == "--profile") {
            configuration.profile = &parse_profile(value());
        } else if (argument == "--protocol") {
            configuration.protocol_filter = parse_protocol(value());
        } else if (argument == "--payload") {
            configuration.payload_filter = parse_payload(value());
        } else if (argument == "--self-test") {
            configuration.warmup = 32;
            configuration.samples = 256;
            configuration.runs = 1;
        } else if (argument == "--help") {
            std::cout
                << "usage: lls_socket_latency_benchmark [--warmup N] "
                   "[--samples N] [--runs N] [--client-cpu N] "
                   "[--server-cpu N] [--udp-timeout-ms N] "
                   "[--profile NAME] [--protocol tcp|udp] "
                   "[--payload 8|64|256|1400] [--self-test]\n";
            std::cout << "profiles:";
            for (const auto& profile : profiles) {
                std::cout << ' ' << profile.name;
            }
            std::cout << '\n';
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown option: " +
                                        std::string{argument});
        }
    }
    constexpr auto maximum_sequence =
        static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max());
    if (configuration.warmup > maximum_sequence ||
        configuration.samples > maximum_sequence - configuration.warmup) {
        throw std::invalid_argument(
            "warmup plus samples exceeds the 32-bit wire sequence");
    }
    if (configuration.udp_timeout_ms > 60'000) {
        throw std::invalid_argument("UDP timeout cannot exceed 60000 ms");
    }
    if (configuration.profile->spin_control_check_interval == 0) {
        throw std::invalid_argument(
            "profile spin control check interval must be positive");
    }
    if (configuration.profile->udp_receive != udp_receive_api::recvmsg &&
        (!configuration.profile->supports_udp ||
         !configuration.profile->udp_connected ||
         configuration.profile->receive_wait != wait_strategy::spin)) {
        throw std::invalid_argument(
            "connected recv profile requires connected UDP spin");
    }
    if (configuration.protocol_filter) {
        const auto supports_selected =
            *configuration.protocol_filter == protocol::tcp
                ? configuration.profile->supports_tcp
                : configuration.profile->supports_udp;
        if (!supports_selected) {
            throw std::invalid_argument(
                "profile does not support the selected protocol");
        }
    } else if (!configuration.profile->supports_tcp ||
               !configuration.profile->supports_udp) {
        throw std::invalid_argument(
            "protocol-specific profile requires --protocol");
    }
    return configuration;
}

struct workload final {
    protocol selected_protocol;
    std::size_t payload_bytes;
};

[[nodiscard]] std::vector<std::size_t> williams_first_row(
    std::size_t width) {
    if (width == 0 || (width > 1 && width % 2 != 0)) {
        throw std::invalid_argument(
            "Williams ordering requires one or an even workload count");
    }
    std::vector<std::size_t> order;
    order.reserve(width);
    for (std::size_t position = 0; position < width; ++position) {
        if (position == 0) {
            order.push_back(0);
        } else if (position % 2 != 0) {
            order.push_back((position + 1) / 2);
        } else {
            order.push_back(width - position / 2);
        }
    }
    return order;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        auto configuration = parse_options(argc, argv);
        resolve_affinity(configuration);
        pin_current_thread(configuration.client_cpu);
        const auto overhead = clock_overhead_p50();
        std::cerr << "clock=CLOCK_MONOTONIC_RAW,clock_overhead_p50_ns="
                  << overhead << ",client_cpu=" << configuration.client_cpu
                  << ",server_cpu=" << configuration.server_cpu
                  << ",scope=linux_ipv4_loopback_socket_profile,profile="
                  << configuration.profile->name << '\n';

        std::vector<workload> canonical_workloads;
        canonical_workloads.reserve(payload_sizes.size() * 2);
        for (const auto payload_bytes : payload_sizes) {
            if ((!configuration.payload_filter ||
                 *configuration.payload_filter == payload_bytes) &&
                (!configuration.protocol_filter ||
                 *configuration.protocol_filter == protocol::tcp)) {
                canonical_workloads.push_back({protocol::tcp, payload_bytes});
            }
            if ((!configuration.payload_filter ||
                 *configuration.payload_filter == payload_bytes) &&
                (!configuration.protocol_filter ||
                 *configuration.protocol_filter == protocol::udp)) {
                canonical_workloads.push_back({protocol::udp, payload_bytes});
            }
        }

        print_header();
        const auto workload_order =
            williams_first_row(canonical_workloads.size());
        for (std::size_t run = 0; run < configuration.runs; ++run) {
            for (std::size_t position = 0;
                 position < workload_order.size();
                 ++position) {
                const auto workload_index =
                    (workload_order[position] + run) %
                    canonical_workloads.size();
                const auto& current = canonical_workloads[workload_index];
                auto result = current.selected_protocol == protocol::tcp
                                  ? run_tcp(current.payload_bytes,
                                            run + 1,
                                            configuration)
                                  : run_udp(current.payload_bytes,
                                            run + 1,
                                            configuration);
                result.workload_position = position + 1;
                print_result(result);
            }
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "socket latency benchmark failed: " << error.what()
                  << '\n';
        return 1;
    }
}
