#include <lls/concurrency/disruptor_single_producer.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <pthread.h>
#include <sched.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/utsname.h>
#include <thread>
#include <utility>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace {

constexpr std::size_t ring_capacity = 65'536;
constexpr std::uint64_t maximum_event_count =
    static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());

struct event final {
    std::uint64_t value{};
};

static_assert(sizeof(event) == 8);

enum class drain_family {
    strict,
    opportunistic,
};

struct config final {
    std::uint64_t events{1'000'000'000};
    std::uint64_t warmup_events{100'000'000};
    std::size_t warmup_runs{2};
    std::size_t producer_batch{1};
    std::size_t drain_limit{1};
    std::string producer_claim_policy{"try-publish-batch"};
    int producer_cpu{-1};
    int consumer_cpu{-1};
    drain_family family{drain_family::strict};
    std::string run_id{"manual"};
    std::string sample_id{"1"};
    std::string pair_order{"unpaired"};
    std::string git_revision{"unknown"};
    std::string build_profile{"unknown"};
    std::string execution_flags{"unknown"};
};

struct validation final {
    std::uint64_t consumed{};
    std::uint64_t checksum{};
    std::uint64_t order_value_mismatch{};
};

struct measurement final {
    std::int64_t duration_ns{};
    int producer_cpu_observed{-1};
    int consumer_cpu_observed{-1};
    validation result{};
};

struct alignas(64) run_control final {
    std::atomic<std::size_t> commanded_phase{0};
    std::atomic<std::size_t> ready_phase{0};
    std::atomic<std::size_t> completed_phase{0};
    std::atomic<bool> failed{false};
    std::uint64_t phase_event_count{};
    measurement phase_measurement{};
};

[[nodiscard]] constexpr std::uint64_t expected_checksum(
    std::uint64_t count) noexcept {
    // Divide before multiplying so this is the exact triangular sum modulo
    // 2^64 rather than an accidentally overflowed intermediate divided by two.
    return (count & 1U) == 0U ? (count / 2U) * (count - 1U)
                              : count * ((count - 1U) / 2U);
}

inline void spin_pause() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

[[nodiscard]] std::uint64_t parse_u64(std::string_view text,
                                      std::string_view option) {
    std::uint64_t value{};
    const auto* const begin = text.data();
    const auto* const end = begin + text.size();
    const auto [parsed_end, error] = std::from_chars(begin, end, value);
    if (error != std::errc{} || parsed_end != end) {
        throw std::invalid_argument{"invalid value for " +
                                    std::string{option} + ": " +
                                    std::string{text}};
    }
    return value;
}

[[nodiscard]] int parse_cpu(std::string_view text, std::string_view option) {
    const auto parsed = parse_u64(text, option);
    if (parsed >= static_cast<std::uint64_t>(CPU_SETSIZE)) {
        throw std::invalid_argument{std::string{option} +
                                    " must be below CPU_SETSIZE"};
    }
    return static_cast<int>(parsed);
}

[[nodiscard]] std::string require_value(int& index,
                                        int argc,
                                        char** argv,
                                        std::string_view option) {
    if (index + 1 >= argc) {
        throw std::invalid_argument{"missing value for " +
                                    std::string{option}};
    }
    ++index;
    return argv[index];
}

[[noreturn]] void print_help_and_exit() {
    std::cout
        << "Usage: lls_disruptor_paired_cpp [options]\n"
        << "  --events N                 measured events (default 1000000000)\n"
        << "  --warmup-events N          events per untimed warm-up\n"
        << "  --warmup-runs N            untimed warm-up count\n"
        << "  --producer-batch P         exact publication claim size\n"
        << "  --claim-policy try-publish-batch\n"
        << "  --drain-limit D            exact/max consumer acknowledgement size\n"
        << "  --family strict|opportunistic\n"
        << "  --producer-cpu ID          required producer CPU\n"
        << "  --consumer-cpu ID          required consumer CPU\n"
        << "  --run-id TEXT --sample-id TEXT --pair-order TEXT\n"
        << "  --git-revision TEXT --build-profile TEXT\n"
        << "  --execution-flags TEXT\n";
    std::exit(EXIT_SUCCESS);
}

[[nodiscard]] config parse_arguments(int argc, char** argv) {
    config output;
    for (int index = 1; index < argc; ++index) {
        const std::string_view option{argv[index]};
        if (option == "--help") {
            print_help_and_exit();
        }
        const auto value = require_value(index, argc, argv, option);
        if (option == "--events") {
            output.events = parse_u64(value, option);
        } else if (option == "--warmup-events") {
            output.warmup_events = parse_u64(value, option);
        } else if (option == "--warmup-runs") {
            output.warmup_runs = static_cast<std::size_t>(parse_u64(value, option));
        } else if (option == "--producer-batch") {
            output.producer_batch =
                static_cast<std::size_t>(parse_u64(value, option));
        } else if (option == "--claim-policy") {
            output.producer_claim_policy = value;
        } else if (option == "--drain-limit") {
            output.drain_limit =
                static_cast<std::size_t>(parse_u64(value, option));
        } else if (option == "--producer-cpu") {
            output.producer_cpu = parse_cpu(value, option);
        } else if (option == "--consumer-cpu") {
            output.consumer_cpu = parse_cpu(value, option);
        } else if (option == "--family") {
            if (value == "strict") {
                output.family = drain_family::strict;
            } else if (value == "opportunistic") {
                output.family = drain_family::opportunistic;
            } else {
                throw std::invalid_argument{"unknown drain family: " + value};
            }
        } else if (option == "--run-id") {
            output.run_id = value;
        } else if (option == "--sample-id") {
            output.sample_id = value;
        } else if (option == "--pair-order") {
            output.pair_order = value;
        } else if (option == "--git-revision") {
            output.git_revision = value;
        } else if (option == "--build-profile") {
            output.build_profile = value;
        } else if (option == "--execution-flags") {
            output.execution_flags = value;
        } else {
            throw std::invalid_argument{"unknown option: " +
                                        std::string{option}};
        }
    }

    if (output.events == 0 || output.events > maximum_event_count) {
        throw std::invalid_argument{"--events must be in [1, INT64_MAX]"};
    }
    if (output.warmup_events == 0 ||
        output.warmup_events > maximum_event_count) {
        throw std::invalid_argument{
            "--warmup-events must be in [1, INT64_MAX]"};
    }
    if (output.warmup_runs == 0) {
        throw std::invalid_argument{"--warmup-runs must be positive"};
    }
    if (output.producer_batch == 0 ||
        output.producer_batch > ring_capacity) {
        throw std::invalid_argument{
            "--producer-batch must be in [1, 65536]"};
    }
    if (output.drain_limit == 0 || output.drain_limit > ring_capacity) {
        throw std::invalid_argument{
            "--drain-limit must be in [1, 65536]"};
    }
    if (output.producer_cpu < 0 || output.consumer_cpu < 0) {
        throw std::invalid_argument{
            "--producer-cpu and --consumer-cpu are required"};
    }
    if (output.producer_cpu == output.consumer_cpu) {
        throw std::invalid_argument{"producer and consumer CPUs must differ"};
    }
    if (output.events % output.producer_batch != 0 ||
        output.warmup_events % output.producer_batch != 0) {
        throw std::invalid_argument{
            "measured and warm-up event counts must be divisible by P so "
            "every producer claim is exactly P"};
    }
    const auto maximum_warmup_runs =
        (maximum_event_count - output.events) / output.warmup_events;
    if (output.warmup_runs > maximum_warmup_runs) {
        throw std::invalid_argument{
            "warm-up plus measured positions must fit in INT64_MAX"};
    }
    if (output.producer_batch != 1 && output.producer_batch != 16 &&
        output.producer_batch != 64) {
        throw std::invalid_argument{"supported P values are 1, 16, and 64"};
    }
    if (output.producer_claim_policy != "try-publish-batch") {
        throw std::invalid_argument{
            "C++ supports --claim-policy try-publish-batch"};
    }
    if (output.drain_limit != 1 && output.drain_limit != 16 &&
        output.drain_limit != 64 && output.drain_limit != ring_capacity) {
        throw std::invalid_argument{
            "supported D values are 1, 16, 64, and 65536"};
    }
    if (output.family == drain_family::strict &&
        output.drain_limit != output.producer_batch) {
        throw std::invalid_argument{"strict family requires D=P"};
    }
    if (output.family == drain_family::opportunistic &&
        output.drain_limit != ring_capacity) {
        throw std::invalid_argument{
            "opportunistic family requires D=ring capacity"};
    }
    return output;
}

[[nodiscard]] int pin_and_verify_current_thread(int requested_cpu) {
    cpu_set_t requested_set;
    CPU_ZERO(&requested_set);
    CPU_SET(static_cast<std::size_t>(requested_cpu), &requested_set);
    const auto set_result = pthread_setaffinity_np(
        pthread_self(), sizeof(requested_set), &requested_set);
    if (set_result != 0) {
        throw std::runtime_error{"pthread_setaffinity_np failed with error " +
                                 std::to_string(set_result)};
    }

    cpu_set_t observed_set;
    CPU_ZERO(&observed_set);
    const auto get_result = pthread_getaffinity_np(
        pthread_self(), sizeof(observed_set), &observed_set);
    if (get_result != 0) {
        throw std::runtime_error{"pthread_getaffinity_np failed with error " +
                                 std::to_string(get_result)};
    }

    std::size_t allowed_count = 0;
    for (std::size_t cpu = 0;
         cpu < static_cast<std::size_t>(CPU_SETSIZE);
         ++cpu) {
        if (CPU_ISSET(cpu, &observed_set) != 0) {
            ++allowed_count;
        }
    }
    if (allowed_count != 1 ||
        CPU_ISSET(static_cast<std::size_t>(requested_cpu), &observed_set) ==
            0) {
        throw std::runtime_error{"affinity mask verification failed"};
    }

    const auto observed_cpu = sched_getcpu();
    if (observed_cpu < 0 || observed_cpu != requested_cpu) {
        throw std::runtime_error{"running CPU verification failed"};
    }
    return observed_cpu;
}

[[nodiscard]] std::int64_t steady_now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

template <std::size_t ProducerBatch, std::size_t DrainLimit>
[[nodiscard]] measurement run_all_phases(const config& settings) {
    static_assert(ProducerBatch == 1 || ProducerBatch == 16 ||
                  ProducerBatch == 64);
    static_assert(DrainLimit == 1 || DrainLimit == 16 || DrainLimit == 64 ||
                  DrainLimit == ring_capacity);
    using stream_type = lls::concurrency::single_producer_disruptor<
        event,
        ring_capacity,
        1>;

    // Pin before ring construction so allocation/zero-fill first-touch is on
    // the same producer NUMA locality in every implementation.
    static_cast<void>(pin_and_verify_current_thread(settings.producer_cpu));
    stream_type stream;
    run_control control;
    std::exception_ptr consumer_error;
    const auto total_phases = settings.warmup_runs + 1;

    std::thread consumer_thread([&] {
        try {
            auto consumer = stream.template make_consumer<0>();
            for (std::size_t phase = 1; phase <= total_phases; ++phase) {
                while (control.commanded_phase.load(std::memory_order_acquire) !=
                       phase) {
                    if (control.failed.load(std::memory_order_acquire)) {
                        return;
                    }
                    spin_pause();
                }

                measurement current;
                current.consumer_cpu_observed =
                    pin_and_verify_current_thread(settings.consumer_cpu);
                const auto first_sequence = consumer.position();
                const auto event_count = control.phase_event_count;
                control.ready_phase.store(phase, std::memory_order_release);

                const auto handler = [&current, first_sequence](
                                         const event& input,
                                         std::uint64_t sequence) noexcept {
                    const auto expected = current.result.consumed;
                    current.result.order_value_mismatch |=
                        (input.value ^ expected) |
                        (sequence ^ (first_sequence + expected));
                    current.result.checksum += input.value;
                    ++current.result.consumed;
                };

                while (current.result.consumed < event_count) {
                    while (consumer.consume_available(DrainLimit, handler) ==
                           0) {
                        if (control.failed.load(std::memory_order_acquire)) {
                            return;
                        }
                        spin_pause();
                    }
                }

                control.phase_measurement = current;
                control.completed_phase.store(phase,
                                              std::memory_order_release);
            }
        } catch (...) {
            consumer_error = std::current_exception();
            control.failed.store(true, std::memory_order_release);
        }
    });

    measurement measured;
    try {
        for (std::size_t phase = 1; phase <= total_phases; ++phase) {
            const auto is_measured = phase == total_phases;
            const auto event_count =
                is_measured ? settings.events : settings.warmup_events;
            control.phase_event_count = event_count;
            control.commanded_phase.store(phase, std::memory_order_release);
            while (control.ready_phase.load(std::memory_order_acquire) != phase) {
                if (control.failed.load(std::memory_order_acquire)) {
                    std::rethrow_exception(consumer_error);
                }
                spin_pause();
            }

            const auto producer_observed =
                pin_and_verify_current_thread(settings.producer_cpu);
            const auto final_consumer_position =
                stream.published_position() + event_count;
            const auto begin_ns = steady_now_ns();
            std::uint64_t published = 0;
            while (published < event_count) {
                const auto writer = [published](event& output,
                                                std::size_t index) noexcept {
                    output.value = published + index;
                };
                while (!stream.try_publish_batch(ProducerBatch, writer)) {
                    if (control.failed.load(std::memory_order_acquire)) {
                        std::rethrow_exception(consumer_error);
                    }
                    spin_pause();
                }
                published += ProducerBatch;
            }

            // Use the same boundary as Java: the producer acquire-observes the
            // final released consumer position, then timestamps immediately.
            while (stream.consumer_position(0) < final_consumer_position) {
                if (control.failed.load(std::memory_order_acquire)) {
                    std::rethrow_exception(consumer_error);
                }
                spin_pause();
            }
            const auto end_ns = steady_now_ns();
            while (control.completed_phase.load(std::memory_order_acquire) !=
                   phase) {
                if (control.failed.load(std::memory_order_acquire)) {
                    std::rethrow_exception(consumer_error);
                }
                spin_pause();
            }
            auto current = control.phase_measurement;
            current.duration_ns = end_ns - begin_ns;
            current.producer_cpu_observed = producer_observed;
            const auto expected = expected_checksum(event_count);
            if (current.duration_ns <= 0 ||
                current.result.consumed != event_count ||
                current.result.checksum != expected ||
                current.result.order_value_mismatch != 0) {
                throw std::runtime_error{is_measured
                                             ? "measured sample failed validation"
                                             : "warm-up failed validation"};
            }
            if (is_measured) {
                measured = current;
            }
        }
    } catch (...) {
        control.failed.store(true, std::memory_order_release);
        consumer_thread.join();
        throw;
    }

    consumer_thread.join();
    if (consumer_error) {
        std::rethrow_exception(consumer_error);
    }
    return measured;
}

template <std::size_t ProducerBatch>
[[nodiscard]] measurement dispatch_drain_limit(const config& settings) {
    switch (settings.drain_limit) {
        case 1:
            return run_all_phases<ProducerBatch, 1>(settings);
        case 16:
            return run_all_phases<ProducerBatch, 16>(settings);
        case 64:
            return run_all_phases<ProducerBatch, 64>(settings);
        case ring_capacity:
            return run_all_phases<ProducerBatch, ring_capacity>(settings);
        default:
            throw std::invalid_argument{
                "supported D values are 1, 16, 64, and 65536"};
    }
}

[[nodiscard]] measurement run_comparison(const config& settings) {
    switch (settings.producer_batch) {
        case 1:
            return dispatch_drain_limit<1>(settings);
        case 16:
            return dispatch_drain_limit<16>(settings);
        case 64:
            return dispatch_drain_limit<64>(settings);
        default:
            throw std::invalid_argument{"supported P values are 1, 16, and 64"};
    }
}

[[nodiscard]] std::string compiler_runtime() {
#if defined(__clang__)
    return "Clang " + std::to_string(__clang_major__) + "." +
           std::to_string(__clang_minor__) + "." +
           std::to_string(__clang_patchlevel__);
#elif defined(__GNUC__)
    return "GCC " + std::to_string(__GNUC__) + "." +
           std::to_string(__GNUC_MINOR__) + "." +
           std::to_string(__GNUC_PATCHLEVEL__);
#else
    return "unknown C++ compiler";
#endif
}

[[nodiscard]] std::string kernel_release() {
    utsname information{};
    if (uname(&information) != 0) {
        return "unknown";
    }
    return information.release;
}

[[nodiscard]] std::string cpu_model() {
    std::ifstream input{"/proc/cpuinfo"};
    std::string line;
    while (std::getline(input, line)) {
        constexpr std::string_view key{"model name"};
        if (line.starts_with(key)) {
            const auto delimiter = line.find(':');
            if (delimiter != std::string::npos) {
                const auto first = line.find_first_not_of(" \t", delimiter + 1);
                return first == std::string::npos ? "unknown"
                                                  : line.substr(first);
            }
        }
    }
    return "unknown";
}

[[nodiscard]] std::string csv_field(std::string_view value) {
    std::string output;
    output.reserve(value.size() + 2);
    output.push_back('"');
    for (const auto character : value) {
        if (character == '\r' || character == '\n') {
            output.push_back(' ');
            continue;
        }
        if (character == '"') {
            output.push_back('"');
        }
        output.push_back(character);
    }
    output.push_back('"');
    return output;
}

[[nodiscard]] std::string hex64(std::uint64_t value) {
    std::ostringstream output;
    output << std::hex << std::setfill('0') << std::setw(16) << value;
    return output.str();
}

void emit_csv(const config& settings, const measurement& measured) {
    static constexpr std::string_view header =
        "schema_version,run_id,sample_id,pair_order,language,implementation,"
        "implementation_version,runtime,build_profile,execution_flags,"
        "dependency_sha256,git_revision,os_name,kernel_release,"
        "cpu_model,ring_capacity,logical_event_bytes,producer_batch,"
        "producer_claim_policy,drain_limit,drain_family,wait_strategy,"
        "producer_cpu_requested,"
        "producer_cpu_observed,consumer_cpu_requested,consumer_cpu_observed,"
        "affinity_verified,warmup_runs,warmup_events,events,consumed_events,"
        "duration_ns,"
        "events_per_second,checksum_hex,expected_checksum_hex,"
        "order_value_mismatch_hex,valid";

    const auto expected = expected_checksum(settings.events);
    const auto valid = measured.duration_ns > 0 &&
                       measured.result.consumed == settings.events &&
                       measured.result.checksum == expected &&
                       measured.result.order_value_mismatch == 0;
    const auto rate = static_cast<double>(settings.events) * 1'000'000'000.0 /
                      static_cast<double>(measured.duration_ns);
    std::ostringstream rate_text;
    rate_text << std::fixed << std::setprecision(3) << rate;

    std::cout << header << '\n'
              << csv_field("1") << ',' << csv_field(settings.run_id) << ','
              << csv_field(settings.sample_id) << ','
              << csv_field(settings.pair_order) << ',' << csv_field("cpp")
              << ',' << csv_field("lls::single_producer_disruptor") << ','
              << csv_field("repository") << ','
              << csv_field(compiler_runtime()) << ','
              << csv_field(settings.build_profile) << ','
              << csv_field(settings.execution_flags) << ','
              << csv_field("n/a") << ','
              << csv_field(settings.git_revision) << ',' << csv_field("Linux")
              << ',' << csv_field(kernel_release()) << ','
              << csv_field(cpu_model()) << ',' << csv_field("65536") << ','
              << csv_field("8") << ','
              << csv_field(std::to_string(settings.producer_batch)) << ','
              << csv_field(settings.producer_claim_policy) << ','
              << csv_field(std::to_string(settings.drain_limit)) << ','
              << csv_field(settings.family == drain_family::strict
                               ? "strict"
                               : "opportunistic")
              << ',' << csv_field("busy-spin-pause") << ','
              << csv_field(std::to_string(settings.producer_cpu)) << ','
              << csv_field(std::to_string(measured.producer_cpu_observed))
              << ',' << csv_field(std::to_string(settings.consumer_cpu)) << ','
              << csv_field(std::to_string(measured.consumer_cpu_observed))
              << ',' << csv_field("true") << ','
              << csv_field(std::to_string(settings.warmup_runs)) << ','
              << csv_field(std::to_string(settings.warmup_events)) << ','
              << csv_field(std::to_string(settings.events)) << ','
              << csv_field(std::to_string(measured.result.consumed)) << ','
              << csv_field(std::to_string(measured.duration_ns)) << ','
              << csv_field(rate_text.str()) << ','
              << csv_field(hex64(measured.result.checksum)) << ','
              << csv_field(hex64(expected)) << ','
              << csv_field(hex64(measured.result.order_value_mismatch)) << ','
              << csv_field(valid ? "true" : "false") << '\n';

    if (!valid) {
        throw std::runtime_error{"measured sample failed validation"};
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const auto settings = parse_arguments(argc, argv);
        emit_csv(settings, run_comparison(settings));
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "paired C++ benchmark: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
