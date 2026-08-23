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
#include <vector>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace {

constexpr std::size_t ring_capacity = 65'536;
constexpr std::uint64_t maximum_event_count =
    static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());

struct event final {
    std::int64_t published_ns{};
};

static_assert(sizeof(event) == 8);

struct config final {
    std::uint64_t events{1'000'000};
    std::uint64_t warmup_events{1'000'000};
    int producer_cpu{-1};
    int consumer_cpu{-1};
    std::string run_id{"manual"};
    std::string sample_id{"1"};
    std::string pair_order{"unpaired"};
    std::string git_revision{"unknown"};
    std::string build_profile{"unknown"};
    std::string execution_flags{"unknown"};
};

struct distribution final {
    std::int64_t p50_ns{};
    std::int64_t p90_ns{};
    std::int64_t p95_ns{};
    std::int64_t p99_ns{};
    std::int64_t p99_9_ns{};
    std::int64_t maximum_ns{};
    std::uint64_t positive_measured_events{};
};

struct measurement final {
    std::uint64_t consumed{};
    std::uint64_t sequence_checksum{};
    std::uint64_t order_mismatch{};
    std::uint64_t nonpositive_latency_count{};
    int producer_cpu_observed{-1};
    int consumer_cpu_observed{-1};
    distribution latency{};
};

[[nodiscard]] constexpr std::uint64_t expected_checksum(
    std::uint64_t count) noexcept {
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

[[nodiscard]] std::int64_t steady_now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
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
        << "Usage: lls_disruptor_paired_latency_cpp [options]\n"
        << "  --events N                 measured events (default 1000000)\n"
        << "  --warmup-events N          untimed warm-up events (default 1000000)\n"
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
        } else if (option == "--producer-cpu") {
            output.producer_cpu = parse_cpu(value, option);
        } else if (option == "--consumer-cpu") {
            output.consumer_cpu = parse_cpu(value, option);
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
    if (output.producer_cpu < 0 || output.consumer_cpu < 0) {
        throw std::invalid_argument{
            "--producer-cpu and --consumer-cpu are required"};
    }
    if (output.producer_cpu == output.consumer_cpu) {
        throw std::invalid_argument{"producer and consumer CPUs must differ"};
    }
    if (output.warmup_events > maximum_event_count - output.events) {
        throw std::invalid_argument{
            "warm-up plus measured positions must fit in INT64_MAX"};
    }
    const auto total = output.warmup_events + output.events;
    if (total > static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())) {
        throw std::invalid_argument{"sample storage exceeds SIZE_MAX"};
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

[[nodiscard]] std::size_t percentile_index(std::size_t count,
                                           std::size_t numerator,
                                           std::size_t denominator) noexcept {
    const auto last = count - 1;
    return (last / denominator) * numerator +
           ((last % denominator) * numerator) / denominator;
}

[[nodiscard]] distribution summarize(const std::vector<std::int64_t>& samples,
                                     std::uint64_t warmup_events,
                                     std::uint64_t measured_events) {
    std::vector<std::int64_t> measured;
    measured.reserve(static_cast<std::size_t>(measured_events));
    const auto total = warmup_events + measured_events;
    for (auto index = warmup_events; index < total; ++index) {
        measured.push_back(samples[static_cast<std::size_t>(index)]);
    }
    std::sort(measured.begin(), measured.end());
    const auto at = [&measured](std::size_t numerator,
                                std::size_t denominator) {
        return measured[percentile_index(
            measured.size(), numerator, denominator)];
    };
    const auto positive = static_cast<std::uint64_t>(std::count_if(
        measured.begin(), measured.end(), [](std::int64_t value) {
            return value > 0;
        }));
    return {
        at(50, 100),
        at(90, 100),
        at(95, 100),
        at(99, 100),
        at(999, 1'000),
        measured.back(),
        positive,
    };
}

[[nodiscard]] measurement run_latency(const config& settings) {
    using stream_type = lls::concurrency::single_producer_disruptor<
        event,
        ring_capacity,
        1>;

    static_cast<void>(pthread_setname_np(pthread_self(), "lls-producer"));
    static_cast<void>(pin_and_verify_current_thread(settings.producer_cpu));
    stream_type stream;
    const auto total = settings.warmup_events + settings.events;
    std::vector<std::int64_t> samples(static_cast<std::size_t>(total));
    std::atomic<bool> ready{false};
    std::atomic<bool> failed{false};
    std::exception_ptr consumer_error;
    measurement result;

    std::thread consumer_thread([&] {
        try {
            static_cast<void>(
                pthread_setname_np(pthread_self(), "lls-consumer"));
            auto consumer = stream.template make_consumer<0>();
            result.consumer_cpu_observed =
                pin_and_verify_current_thread(settings.consumer_cpu);
            ready.store(true, std::memory_order_release);

            const auto handler = [&](const event& input,
                                     std::uint64_t sequence) noexcept {
                const auto received_ns = steady_now_ns();
                const auto expected = result.consumed;
                const auto elapsed_ns = received_ns - input.published_ns;
                samples[static_cast<std::size_t>(expected)] = elapsed_ns;
                result.order_mismatch |= sequence ^ expected;
                result.sequence_checksum += sequence;
                result.nonpositive_latency_count +=
                    elapsed_ns > 0 ? 0U : 1U;
                result.consumed = expected + 1U;
            };

            while (result.consumed < total) {
                while (consumer.consume_available(1, handler) == 0) {
                    if (failed.load(std::memory_order_acquire)) {
                        return;
                    }
                    spin_pause();
                }
            }
        } catch (...) {
            consumer_error = std::current_exception();
            failed.store(true, std::memory_order_release);
        }
    });

    try {
        while (!ready.load(std::memory_order_acquire)) {
            if (failed.load(std::memory_order_acquire)) {
                std::rethrow_exception(consumer_error);
            }
            spin_pause();
        }
        result.producer_cpu_observed =
            pin_and_verify_current_thread(settings.producer_cpu);

        for (std::uint64_t published = 0; published < total; ++published) {
            const auto writer = [](event& output,
                                   std::size_t) noexcept {
                // try_publish_batch establishes the claim before invoking the
                // writer, so this timestamp excludes capacity-claim retries.
                output.published_ns = steady_now_ns();
            };
            while (!stream.try_publish_batch(1, writer)) {
                if (failed.load(std::memory_order_acquire)) {
                    std::rethrow_exception(consumer_error);
                }
                spin_pause();
            }
            const auto released_position = published + 1U;
            while (stream.consumer_position(0) < released_position) {
                if (failed.load(std::memory_order_acquire)) {
                    std::rethrow_exception(consumer_error);
                }
                spin_pause();
            }
        }
    } catch (...) {
        failed.store(true, std::memory_order_release);
        consumer_thread.join();
        throw;
    }

    consumer_thread.join();
    if (consumer_error) {
        std::rethrow_exception(consumer_error);
    }
    const auto expected = expected_checksum(total);
    if (result.consumed != total || result.sequence_checksum != expected ||
        result.order_mismatch != 0 ||
        result.nonpositive_latency_count != 0) {
        throw std::runtime_error{"latency sample failed validation"};
    }
    result.latency = summarize(samples, settings.warmup_events, settings.events);
    if (result.latency.positive_measured_events != settings.events) {
        throw std::runtime_error{"measured latency sample is not all-positive"};
    }
    return result;
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
        constexpr std::string_view prefix{"model name"};
        if (line.starts_with(prefix)) {
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

[[nodiscard]] std::string csv_field(std::string value) {
    std::string escaped;
    escaped.reserve(value.size() + 2);
    escaped.push_back('"');
    for (const auto character : value) {
        if (character == '"') {
            escaped += "\"\"";
        } else if (character == '\r' || character == '\n') {
            escaped.push_back(' ');
        } else {
            escaped.push_back(character);
        }
    }
    escaped.push_back('"');
    return escaped;
}

[[nodiscard]] std::string hex64(std::uint64_t value) {
    std::ostringstream output;
    output << std::hex;
    output.width(16);
    output.fill('0');
    output << value;
    return output.str();
}

void emit_csv(const config& settings, const measurement& result) {
    std::cout
        << "schema_version,benchmark,run_id,sample_id,pair_order,language,"
           "implementation,implementation_version,runtime,build_profile,"
           "execution_flags,dependency_sha256,git_revision,os_name,"
           "kernel_release,cpu_model,ring_capacity,logical_event_bytes,"
           "producer_batch,producer_claim_policy,drain_limit,drain_family,"
           "wait_strategy,wait_primitive,latency_boundary,"
           "queue_residence_policy,clock,"
           "producer_cpu_requested,producer_cpu_observed,"
           "consumer_cpu_requested,consumer_cpu_observed,affinity_verified,"
           "warmup_events,events,consumed_events,positive_measured_events,"
           "sequence_checksum_hex,expected_sequence_checksum_hex,"
           "order_mismatch_hex,p50_ns,p90_ns,p95_ns,p99_ns,p99_9_ns,max_ns,"
           "valid\n";

    const auto total = settings.warmup_events + settings.events;
    const std::vector<std::string> fields{
        "1",
        "paired-handoff-latency",
        settings.run_id,
        settings.sample_id,
        settings.pair_order,
        "cpp",
        "lls single_producer_disruptor",
        "workspace-header",
        compiler_runtime(),
        settings.build_profile,
        settings.execution_flags,
        "none",
        settings.git_revision,
        "Linux",
        kernel_release(),
        cpu_model(),
        std::to_string(ring_capacity),
        "8",
        "1",
        "try-publish-batch",
        "1",
        "strict",
        "busy-spin-pause",
        "x86 PAUSE",
        "post-claim producer timestamp to consumer handler-entry timestamp",
        "producer acquire-waits for consumer release after every event",
        "std::chrono::steady_clock",
        std::to_string(settings.producer_cpu),
        std::to_string(result.producer_cpu_observed),
        std::to_string(settings.consumer_cpu),
        std::to_string(result.consumer_cpu_observed),
        "true",
        std::to_string(settings.warmup_events),
        std::to_string(settings.events),
        std::to_string(result.consumed),
        std::to_string(result.latency.positive_measured_events),
        hex64(result.sequence_checksum),
        hex64(expected_checksum(total)),
        hex64(result.order_mismatch),
        std::to_string(result.latency.p50_ns),
        std::to_string(result.latency.p90_ns),
        std::to_string(result.latency.p95_ns),
        std::to_string(result.latency.p99_ns),
        std::to_string(result.latency.p99_9_ns),
        std::to_string(result.latency.maximum_ns),
        "true",
    };
    for (std::size_t index = 0; index < fields.size(); ++index) {
        if (index != 0) {
            std::cout << ',';
        }
        std::cout << csv_field(fields[index]);
    }
    std::cout << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const auto settings = parse_arguments(argc, argv);
        const auto result = run_latency(settings);
        emit_csv(settings, result);
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "paired C++ latency benchmark: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
