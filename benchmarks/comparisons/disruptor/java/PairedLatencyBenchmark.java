import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Workload-matched handoff-latency comparator for LMAX Disruptor 4.0.0. */
public final class PairedLatencyBenchmark {
    private static final int RING_CAPACITY = 65_536;
    private static final String DISRUPTOR_VERSION = "4.0.0";
    private static final long AFFINITY_TIMEOUT_NANOS = 30_000_000_000L;

    private PairedLatencyBenchmark() {
    }

    private static final class Event {
        private long publishedNanos;
    }

    private static final class Config {
        private long events = 1_000_000L;
        private long warmupEvents = 1_000_000L;
        private int producerCpu = -1;
        private int consumerCpu = -1;
        private String runId = "manual";
        private String sampleId = "1";
        private String pairOrder = "unpaired";
        private String gitRevision = "unknown";
        private String buildProfile = "server-jit";
        private String executionFlags = "unknown";
        private String dependencySha256 = "unknown";
        private Path affinityGate;
    }

    private static final class LatencyHandler implements EventHandler<Event> {
        private final long[] samples;
        private final AtomicBoolean ready;
        private long consumed;
        private long sequenceChecksum;
        private long orderMismatch;
        private long nonpositiveLatencyCount;

        LatencyHandler(final long[] allSamples, final AtomicBoolean isReady) {
            samples = allSamples;
            ready = isReady;
        }

        @Override
        public void onStart() {
            ready.set(true);
        }

        @Override
        public void onEvent(
                final Event event,
                final long sequence,
                final boolean endOfBatch) {
            final long receivedNanos = System.nanoTime();
            final long expected = consumed;
            final long elapsedNanos = receivedNanos - event.publishedNanos;
            samples[(int) expected] = elapsedNanos;
            orderMismatch |= sequence ^ expected;
            sequenceChecksum += sequence;
            nonpositiveLatencyCount += elapsedNanos > 0L ? 0L : 1L;
            consumed = expected + 1L;
        }
    }

    private static final class Phase {
        private final RingBuffer<Event> ring;
        private final BatchEventProcessor<Event> processor;
        private final LatencyHandler handler;

        Phase(final long[] samples, final AtomicBoolean ready) {
            ring = RingBuffer.createSingleProducer(
                    Event::new, RING_CAPACITY, new BusySpinWaitStrategy());
            final SequenceBarrier barrier = ring.newBarrier();
            handler = new LatencyHandler(samples, ready);
            processor = new BatchEventProcessorBuilder()
                    .setMaxBatchSize(1)
                    .build(ring, barrier, handler);
            ring.addGatingSequences(processor.getSequence());
        }
    }

    private static final class Distribution {
        private long p50Nanos;
        private long p90Nanos;
        private long p95Nanos;
        private long p99Nanos;
        private long p99_9Nanos;
        private long maximumNanos;
        private long positiveMeasuredEvents;
    }

    private static final class Measurement {
        private long consumed;
        private long sequenceChecksum;
        private long orderMismatch;
        private long nonpositiveLatencyCount;
        private int producerObservedCpu;
        private int consumerObservedCpu;
        private Distribution latency;
    }

    public static void main(final String[] arguments) {
        try {
            final Config config = parseArguments(arguments);
            Thread.currentThread().setName("lls-producer");
            final int total = Math.toIntExact(
                    Math.addExact(config.warmupEvents, config.events));
            final AtomicBoolean ready = new AtomicBoolean();
            final AtomicBoolean failed = new AtomicBoolean();
            final AtomicReference<Throwable> consumerError =
                    new AtomicReference<>();
            final AtomicInteger consumerObserved = new AtomicInteger(-1);
            final AtomicReference<Phase> phaseSlot = new AtomicReference<>();

            final Thread consumerThread = new Thread(() -> {
                try {
                    awaitAffinityGate(config.affinityGate);
                    consumerObserved.set(
                            waitForAndVerifyAffinity(config.consumerCpu));
                    Phase phase;
                    while ((phase = phaseSlot.get()) == null) {
                        if (failed.get()) {
                            return;
                        }
                        Thread.onSpinWait();
                    }
                    phase.processor.run();
                } catch (final Throwable error) {
                    consumerError.set(error);
                    failed.set(true);
                }
            }, "lls-consumer");
            consumerThread.start();

            Phase phase = null;
            try {
                // Both named hot threads wait behind one gate. The launcher
                // pins them exactly and moves all JVM helpers to housekeeping.
                awaitAffinityGate(config.affinityGate);
                staticCastUnused(waitForAndVerifyAffinity(config.producerCpu));
                final long[] allSamples = new long[total];
                // Allocate the retained ring only after producer affinity is
                // exact, matching native first-touch placement.
                phase = new Phase(allSamples, ready);
                phaseSlot.set(phase);
                while (!ready.get()) {
                    rethrowConsumerFailure(failed, consumerError);
                    Thread.onSpinWait();
                }
                final int producerObserved =
                        waitForAndVerifyAffinity(config.producerCpu);
                final Sequence consumerSequence = phase.processor.getSequence();

                for (long published = 0L; published < total; ++published) {
                    long sequence;
                    for (;;) {
                        try {
                            sequence = phase.ring.tryNext();
                            break;
                        } catch (final InsufficientCapacityException full) {
                            rethrowConsumerFailure(failed, consumerError);
                            Thread.onSpinWait();
                        }
                    }
                    // The capacity claim succeeded before this timestamp.
                    phase.ring.get(sequence).publishedNanos = System.nanoTime();
                    phase.ring.publish(sequence);
                    while (consumerSequence.get() < sequence) {
                        rethrowConsumerFailure(failed, consumerError);
                        Thread.onSpinWait();
                    }
                }

                // The final acquire observation above proves that the handler
                // returned and BatchEventProcessor released its sequence.
                phase.processor.halt();
                consumerThread.join();
                rethrowConsumerFailure(failed, consumerError);

                final Measurement result = new Measurement();
                result.consumed = phase.handler.consumed;
                result.sequenceChecksum = phase.handler.sequenceChecksum;
                result.orderMismatch = phase.handler.orderMismatch;
                result.nonpositiveLatencyCount =
                        phase.handler.nonpositiveLatencyCount;
                result.producerObservedCpu = producerObserved;
                result.consumerObservedCpu = consumerObserved.get();
                verify(config, result, total);
                result.latency = summarize(allSamples, config, total);
                if (result.latency.positiveMeasuredEvents != config.events) {
                    throw new IllegalStateException(
                            "measured latency sample is not all-positive");
                }
                emitCsv(config, result);
            } catch (final Throwable error) {
                failed.set(true);
                if (phase != null) {
                    phase.processor.halt();
                }
                consumerThread.join();
                throw error;
            }
        } catch (final Throwable error) {
            System.err.println(
                    "paired Java latency benchmark: " + error.getMessage());
            System.exit(1);
        }
    }

    private static Config parseArguments(final String[] arguments) {
        final Config config = new Config();
        for (int index = 0; index < arguments.length; ++index) {
            final String option = arguments[index];
            if ("--help".equals(option)) {
                printHelpAndExit();
            }
            if (index + 1 >= arguments.length) {
                throw new IllegalArgumentException("missing value for " + option);
            }
            final String value = arguments[++index];
            switch (option) {
                case "--events" -> config.events = parsePositiveLong(value, option);
                case "--warmup-events" ->
                        config.warmupEvents = parsePositiveLong(value, option);
                case "--producer-cpu" ->
                        config.producerCpu = parseNonNegativeInt(value, option);
                case "--consumer-cpu" ->
                        config.consumerCpu = parseNonNegativeInt(value, option);
                case "--run-id" -> config.runId = value;
                case "--sample-id" -> config.sampleId = value;
                case "--pair-order" -> config.pairOrder = value;
                case "--git-revision" -> config.gitRevision = value;
                case "--build-profile" -> config.buildProfile = value;
                case "--execution-flags" -> config.executionFlags = value;
                case "--dependency-sha256" -> config.dependencySha256 = value;
                case "--affinity-gate" -> config.affinityGate = Path.of(value);
                default -> throw new IllegalArgumentException(
                        "unknown option: " + option);
            }
        }
        if (config.producerCpu < 0 || config.consumerCpu < 0) {
            throw new IllegalArgumentException(
                    "--producer-cpu and --consumer-cpu are required");
        }
        if (config.producerCpu == config.consumerCpu) {
            throw new IllegalArgumentException(
                    "producer and consumer CPUs must differ");
        }
        if (config.affinityGate == null) {
            throw new IllegalArgumentException(
                    "--affinity-gate is required; use run_paired_latency.py");
        }
        try {
            final long total = Math.addExact(config.warmupEvents, config.events);
            if (total > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "warm-up plus measured samples must fit in a Java array");
            }
        } catch (final ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "warm-up plus measured samples overflow", overflow);
        }
        return config;
    }

    private static long parsePositiveLong(
            final String value, final String option) {
        final long parsed = Long.parseLong(value);
        if (parsed <= 0L) {
            throw new IllegalArgumentException(option + " must be positive");
        }
        return parsed;
    }

    private static int parseNonNegativeInt(
            final String value, final String option) {
        final int parsed = Integer.parseInt(value);
        if (parsed < 0) {
            throw new IllegalArgumentException(option + " must be non-negative");
        }
        return parsed;
    }

    private static void printHelpAndExit() {
        System.out.println("Usage: PairedLatencyBenchmark [options]");
        System.out.println("  --events N --warmup-events N");
        System.out.println("  --producer-cpu ID --consumer-cpu ID");
        System.out.println(
                "  --affinity-gate PATH (managed by run_paired_latency.py)");
        System.exit(0);
    }

    private static void rethrowConsumerFailure(
            final AtomicBoolean failed,
            final AtomicReference<Throwable> consumerError) throws Throwable {
        if (failed.get()) {
            final Throwable error = consumerError.get();
            if (error == null) {
                throw new IllegalStateException("consumer failed");
            }
            throw error;
        }
    }

    private static void staticCastUnused(final int ignored) {
        // Named no-op documents an affinity verification used only for gating.
    }

    private static void awaitAffinityGate(final Path gate) {
        final long deadline = System.nanoTime() + AFFINITY_TIMEOUT_NANOS;
        while (!Files.exists(gate)) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new IllegalStateException("affinity gate timed out");
            }
            Thread.onSpinWait();
        }
    }

    private static int waitForAndVerifyAffinity(final int requestedCpu)
            throws IOException {
        final String expectedMask = Integer.toString(requestedCpu);
        final long deadline = System.nanoTime() + AFFINITY_TIMEOUT_NANOS;
        for (;;) {
            final List<String> status =
                    Files.readAllLines(Path.of("/proc/thread-self/status"));
            String allowed = null;
            for (final String line : status) {
                if (line.startsWith("Cpus_allowed_list:")) {
                    allowed = line.substring(line.indexOf(':') + 1).trim();
                    break;
                }
            }
            if (expectedMask.equals(allowed)) {
                final int observed = currentProcessorFromProcStat();
                if (observed != requestedCpu) {
                    throw new IllegalStateException(
                            "affinity mask is exact but current CPU is " + observed);
                }
                return observed;
            }
            if (System.nanoTime() - deadline >= 0L) {
                throw new IllegalStateException(
                        "affinity verification timed out; observed mask="
                                + allowed + ", requested=" + expectedMask);
            }
            Thread.onSpinWait();
        }
    }

    private static int currentProcessorFromProcStat() throws IOException {
        final String stat = Files.readString(
                Path.of("/proc/thread-self/stat")).trim();
        final int commandEnd = stat.lastIndexOf(')');
        if (commandEnd < 0 || commandEnd + 2 >= stat.length()) {
            throw new IllegalStateException("cannot parse /proc/thread-self/stat");
        }
        final String[] fields = stat.substring(commandEnd + 2).split("\\s+");
        if (fields.length <= 36) {
            throw new IllegalStateException(
                    "processor field missing from proc stat");
        }
        return Integer.parseInt(fields[36]);
    }

    private static long expectedChecksum(final long count) {
        return (count & 1L) == 0L
                ? (count / 2L) * (count - 1L)
                : count * ((count - 1L) / 2L);
    }

    private static void verify(
            final Config config,
            final Measurement result,
            final long total) {
        if (result.consumed != total
                || result.sequenceChecksum != expectedChecksum(total)
                || result.orderMismatch != 0L
                || result.nonpositiveLatencyCount != 0L
                || result.producerObservedCpu != config.producerCpu
                || result.consumerObservedCpu != config.consumerCpu) {
            throw new IllegalStateException("latency sample failed validation");
        }
    }

    private static int percentileIndex(
            final int count, final int numerator, final int denominator) {
        final int last = count - 1;
        return (last / denominator) * numerator
                + ((last % denominator) * numerator) / denominator;
    }

    private static Distribution summarize(
            final long[] allSamples,
            final Config config,
            final int total) {
        final int first = Math.toIntExact(config.warmupEvents);
        final long[] measured = Arrays.copyOfRange(allSamples, first, total);
        Arrays.sort(measured);
        final Distribution output = new Distribution();
        output.p50Nanos = measured[percentileIndex(measured.length, 50, 100)];
        output.p90Nanos = measured[percentileIndex(measured.length, 90, 100)];
        output.p95Nanos = measured[percentileIndex(measured.length, 95, 100)];
        output.p99Nanos = measured[percentileIndex(measured.length, 99, 100)];
        output.p99_9Nanos = measured[
                percentileIndex(measured.length, 999, 1_000)];
        output.maximumNanos = measured[measured.length - 1];
        long positive = 0L;
        for (final long value : measured) {
            positive += value > 0L ? 1L : 0L;
        }
        output.positiveMeasuredEvents = positive;
        return output;
    }

    private static String kernelRelease() {
        try {
            return Files.readString(Path.of("/proc/sys/kernel/osrelease")).trim();
        } catch (final IOException ignored) {
            return "unknown";
        }
    }

    private static String cpuModel() {
        try {
            for (final String line : Files.readAllLines(Path.of("/proc/cpuinfo"))) {
                if (line.startsWith("model name")) {
                    final int delimiter = line.indexOf(':');
                    if (delimiter >= 0) {
                        return line.substring(delimiter + 1).trim();
                    }
                }
            }
        } catch (final IOException ignored) {
            // Emit an explicit metadata placeholder.
        }
        return "unknown";
    }

    private static String csvField(final Object input) {
        final String value = String.valueOf(input)
                .replace('\r', ' ')
                .replace('\n', ' ')
                .replace("\"", "\"\"");
        return '"' + value + '"';
    }

    private static String hex64(final long value) {
        return String.format(Locale.ROOT, "%016x", value);
    }

    private static void emitCsv(
            final Config config, final Measurement result) {
        final String header = String.join(",",
                "schema_version", "benchmark", "run_id", "sample_id",
                "pair_order", "language", "implementation",
                "implementation_version", "runtime", "build_profile",
                "execution_flags", "dependency_sha256", "git_revision",
                "os_name", "kernel_release", "cpu_model", "ring_capacity",
                "logical_event_bytes", "producer_batch",
                "producer_claim_policy", "drain_limit", "drain_family",
                "wait_strategy", "wait_primitive", "latency_boundary",
                "queue_residence_policy", "clock", "producer_cpu_requested",
                "producer_cpu_observed", "consumer_cpu_requested",
                "consumer_cpu_observed", "affinity_verified", "warmup_events",
                "events", "consumed_events", "positive_measured_events",
                "sequence_checksum_hex", "expected_sequence_checksum_hex",
                "order_mismatch_hex", "p50_ns", "p90_ns", "p95_ns",
                "p99_ns", "p99_9_ns", "max_ns", "valid");
        final long total = config.warmupEvents + config.events;
        final List<Object> fields = new ArrayList<>();
        fields.add("1");
        fields.add("paired-handoff-latency");
        fields.add(config.runId);
        fields.add(config.sampleId);
        fields.add(config.pairOrder);
        fields.add("java");
        fields.add("LMAX Disruptor");
        fields.add(DISRUPTOR_VERSION);
        fields.add(System.getProperty("java.vm.name") + " " + Runtime.version());
        fields.add(config.buildProfile);
        fields.add(config.executionFlags);
        fields.add(config.dependencySha256);
        fields.add(config.gitRevision);
        fields.add("Linux");
        fields.add(kernelRelease());
        fields.add(cpuModel());
        fields.add(RING_CAPACITY);
        fields.add(8);
        fields.add(1);
        fields.add("try-next");
        fields.add(1);
        fields.add("strict");
        fields.add("busy-spin-pause");
        fields.add("BusySpinWaitStrategy/Thread.onSpinWait");
        fields.add(
                "post-claim producer timestamp to consumer handler-entry timestamp");
        fields.add("producer acquire-waits for consumer release after every event");
        fields.add("System.nanoTime");
        fields.add(config.producerCpu);
        fields.add(result.producerObservedCpu);
        fields.add(config.consumerCpu);
        fields.add(result.consumerObservedCpu);
        fields.add(true);
        fields.add(config.warmupEvents);
        fields.add(config.events);
        fields.add(result.consumed);
        fields.add(result.latency.positiveMeasuredEvents);
        fields.add(hex64(result.sequenceChecksum));
        fields.add(hex64(expectedChecksum(total)));
        fields.add(hex64(result.orderMismatch));
        fields.add(result.latency.p50Nanos);
        fields.add(result.latency.p90Nanos);
        fields.add(result.latency.p95Nanos);
        fields.add(result.latency.p99Nanos);
        fields.add(result.latency.p99_9Nanos);
        fields.add(result.latency.maximumNanos);
        fields.add(true);

        System.out.println(header);
        System.out.println(fields.stream()
                .map(PairedLatencyBenchmark::csvField)
                .reduce((left, right) -> left + ',' + right)
                .orElseThrow());
    }
}
