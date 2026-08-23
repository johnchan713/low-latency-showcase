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
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Same-work comparator for LMAX Disruptor 4.0.0. */
public final class PairedBenchmark {
    private static final int RING_CAPACITY = 65_536;
    private static final String DISRUPTOR_VERSION = "4.0.0";
    private static final long AFFINITY_TIMEOUT_NANOS = 30_000_000_000L;

    private PairedBenchmark() {
    }

    private enum DrainFamily {
        STRICT("strict"),
        OPPORTUNISTIC("opportunistic");

        private final String csvName;

        DrainFamily(final String csvName) {
            this.csvName = csvName;
        }
    }

    private enum ClaimPolicy {
        TRY_NEXT("try-next"),
        BLOCKING_NEXT("blocking-next");

        private final String csvName;

        ClaimPolicy(final String csvName) {
            this.csvName = csvName;
        }
    }

    private static final class Event {
        private long value;
    }

    private static final class Config {
        private long events = 1_000_000_000L;
        private long warmupEvents = 100_000_000L;
        private int warmupRuns = 2;
        private int producerBatch = 1;
        private int drainLimit = 1;
        private ClaimPolicy claimPolicy = ClaimPolicy.TRY_NEXT;
        private int producerCpu = -1;
        private int consumerCpu = -1;
        private DrainFamily family = DrainFamily.STRICT;
        private String runId = "manual";
        private String sampleId = "1";
        private String pairOrder = "unpaired";
        private String gitRevision = "unknown";
        private String buildProfile = "server-jit";
        private String executionFlags = "unknown";
        private String dependencySha256 = "unknown";
        private Path affinityGate;
    }

    private static final class ValidationHandler implements EventHandler<Event> {
        private final AtomicInteger readyPhase;
        private long firstSequence;
        private long consumed;
        private long checksum;
        private long orderValueMismatch;
        private int phaseNumber;

        ValidationHandler(final AtomicInteger readyPhase) {
            this.readyPhase = readyPhase;
        }

        void prepare(
                final long phaseFirstSequence,
                final int currentPhase) {
            firstSequence = phaseFirstSequence;
            consumed = 0L;
            checksum = 0L;
            orderValueMismatch = 0L;
            phaseNumber = currentPhase;
        }

        @Override
        public void onStart() {
            // Signal only after BatchEventProcessor has entered RUNNING,
            // cleared its prior halt alert, and reached its start callback.
            readyPhase.set(phaseNumber);
        }

        @Override
        public void onEvent(
                final Event event,
                final long sequence,
                final boolean endOfBatch) {
            final long expected = consumed;
            orderValueMismatch |= (event.value ^ expected)
                    | (sequence ^ (firstSequence + expected));
            checksum += event.value;
            consumed = expected + 1L;
        }
    }

    private static final class Phase {
        private final RingBuffer<Event> ring;
        private final BatchEventProcessor<Event> processor;
        private final ValidationHandler handler;

        Phase(final int drainLimit, final AtomicInteger readyPhase) {
            ring = RingBuffer.createSingleProducer(
                    Event::new, RING_CAPACITY, new BusySpinWaitStrategy());
            final SequenceBarrier barrier = ring.newBarrier();
            handler = new ValidationHandler(readyPhase);
            processor = new BatchEventProcessorBuilder()
                    .setMaxBatchSize(drainLimit)
                    .build(ring, barrier, handler);
            ring.addGatingSequences(processor.getSequence());
        }
    }

    private static final class Measurement {
        private long durationNanos;
        private long consumed;
        private long checksum;
        private long orderValueMismatch;
        private int producerObservedCpu;
        private int consumerObservedCpu;
    }

    public static void main(final String[] arguments) {
        try {
            final Config config = parseArguments(arguments);
            Thread.currentThread().setName("lls-producer");

            final int totalPhases = config.warmupRuns + 1;
            final AtomicInteger commandedPhase = new AtomicInteger();
            final AtomicInteger readyPhase = new AtomicInteger();
            final AtomicInteger completedPhase = new AtomicInteger();
            final AtomicBoolean failed = new AtomicBoolean();
            final AtomicReference<Throwable> consumerError =
                    new AtomicReference<>();
            final AtomicInteger consumerObserved = new AtomicInteger(-1);
            final AtomicReference<Phase> phaseSlot = new AtomicReference<>();

            final Thread consumerThread = new Thread(() -> {
                try {
                    awaitAffinityGate(config.affinityGate);
                    for (int phaseNumber = 1;
                         phaseNumber <= totalPhases;
                         ++phaseNumber) {
                        while (commandedPhase.get() != phaseNumber) {
                            Thread.onSpinWait();
                        }
                        consumerObserved.set(
                                waitForAndVerifyAffinity(config.consumerCpu));
                        final Phase phase = phaseSlot.get();
                        phase.processor.run();
                        completedPhase.set(phaseNumber);
                    }
                } catch (final Throwable error) {
                    consumerError.set(error);
                    failed.set(true);
                }
            }, "lls-consumer");
            consumerThread.start();
            // Both named hot threads now exist behind the same closed gate.
            // run_paired.py pins them individually, moves JVM helpers to
            // housekeeping CPUs, then opens the gate once.
            awaitAffinityGate(config.affinityGate);
            static_castUnused(waitForAndVerifyAffinity(config.producerCpu));
            // Construct/zero the single retained ring only after the producer
            // has exact affinity, matching C++ first-touch locality.
            final Phase phase = new Phase(config.drainLimit, readyPhase);
            phaseSlot.set(phase);

            Measurement measured = null;
            for (int phaseNumber = 1;
                 phaseNumber <= totalPhases;
                 ++phaseNumber) {
                final boolean isMeasured = phaseNumber == totalPhases;
                final long eventCount =
                        isMeasured ? config.events : config.warmupEvents;
                final long firstSequence = phase.processor.getSequence().get() + 1L;
                phase.handler.prepare(firstSequence, phaseNumber);
                commandedPhase.set(phaseNumber);
                while (readyPhase.get() != phaseNumber) {
                    rethrowConsumerFailure(failed, consumerError);
                    Thread.onSpinWait();
                }

                final int producerObserved =
                        waitForAndVerifyAffinity(config.producerCpu);
                final Sequence consumerSequence =
                        phase.processor.getSequence();
                final long finalSequence = firstSequence + eventCount - 1L;
                final long beginNanos = System.nanoTime();
                publishExactly(config, phase.ring, eventCount, failed,
                        consumerError);
                while (consumerSequence.get() < finalSequence) {
                    rethrowConsumerFailure(failed, consumerError);
                    Thread.onSpinWait();
                }
                // Match C++: timestamp immediately after the producer's
                // acquire observation of the final consumer release.
                final long endNanos = System.nanoTime();
                phase.processor.halt();
                while (completedPhase.get() != phaseNumber) {
                    rethrowConsumerFailure(failed, consumerError);
                    Thread.onSpinWait();
                }

                final Measurement phaseResult = new Measurement();
                phaseResult.durationNanos = endNanos - beginNanos;
                phaseResult.consumed = phase.handler.consumed;
                phaseResult.checksum = phase.handler.checksum;
                phaseResult.orderValueMismatch =
                        phase.handler.orderValueMismatch;
                phaseResult.producerObservedCpu = producerObserved;
                phaseResult.consumerObservedCpu = consumerObserved.get();
                verify(config, phaseResult, eventCount, isMeasured
                        ? "measured sample" : "warm-up");
                if (isMeasured) {
                    measured = phaseResult;
                }
            }

            consumerThread.join();
            rethrowConsumerFailure(failed, consumerError);
            if (measured == null) {
                throw new IllegalStateException("measured phase did not run");
            }
            emitCsv(config, measured);
        } catch (final Throwable error) {
            System.err.println("paired Java benchmark: " + error.getMessage());
            System.exit(1);
        }
    }

    private static void publishExactly(
            final Config config,
            final RingBuffer<Event> ring,
            final long eventCount,
            final AtomicBoolean failed,
            final AtomicReference<Throwable> consumerError) throws Throwable {
        switch (config.producerBatch) {
            case 1 -> {
                if (config.claimPolicy == ClaimPolicy.TRY_NEXT) {
                    publishTryP1(ring, eventCount, failed, consumerError);
                } else {
                    publishBlockingP1(ring, eventCount);
                }
            }
            case 16 -> {
                if (config.claimPolicy == ClaimPolicy.TRY_NEXT) {
                    publishTryP16(ring, eventCount, failed, consumerError);
                } else {
                    publishBlockingP16(ring, eventCount);
                }
            }
            case 64 -> {
                if (config.claimPolicy == ClaimPolicy.TRY_NEXT) {
                    publishTryP64(ring, eventCount, failed, consumerError);
                } else {
                    publishBlockingP64(ring, eventCount);
                }
            }
            default -> throw new IllegalArgumentException(
                    "supported P values are 1, 16, and 64");
        }
    }

    private static void publishTryP1(
            final RingBuffer<Event> ring,
            final long eventCount,
            final AtomicBoolean failed,
            final AtomicReference<Throwable> consumerError) throws Throwable {
        long published = 0L;
        while (published < eventCount) {
            long high;
            for (;;) {
                try {
                    high = ring.tryNext();
                    break;
                } catch (final InsufficientCapacityException full) {
                    rethrowConsumerFailure(failed, consumerError);
                    Thread.onSpinWait();
                }
            }
            ring.get(high).value = published;
            ring.publish(high);
            ++published;
        }
    }

    private static void publishTryP16(
            final RingBuffer<Event> ring,
            final long eventCount,
            final AtomicBoolean failed,
            final AtomicReference<Throwable> consumerError) throws Throwable {
        long published = 0L;
        while (published < eventCount) {
            long high;
            for (;;) {
                try {
                    high = ring.tryNext(16);
                    break;
                } catch (final InsufficientCapacityException full) {
                    rethrowConsumerFailure(failed, consumerError);
                    Thread.onSpinWait();
                }
            }
            final long low = high - 15L;
            for (int index = 0; index < 16; ++index) {
                ring.get(low + index).value = published + index;
            }
            ring.publish(low, high);
            published += 16L;
        }
    }

    private static void publishTryP64(
            final RingBuffer<Event> ring,
            final long eventCount,
            final AtomicBoolean failed,
            final AtomicReference<Throwable> consumerError) throws Throwable {
        long published = 0L;
        while (published < eventCount) {
            long high;
            for (;;) {
                try {
                    high = ring.tryNext(64);
                    break;
                } catch (final InsufficientCapacityException full) {
                    rethrowConsumerFailure(failed, consumerError);
                    Thread.onSpinWait();
                }
            }
            final long low = high - 63L;
            for (int index = 0; index < 64; ++index) {
                ring.get(low + index).value = published + index;
            }
            ring.publish(low, high);
            published += 64L;
        }
    }

    private static void publishBlockingP1(
            final RingBuffer<Event> ring,
            final long eventCount) {
        long published = 0L;
        while (published < eventCount) {
            final long sequence = ring.next();
            ring.get(sequence).value = published;
            ring.publish(sequence);
            ++published;
        }
    }

    private static void publishBlockingP16(
            final RingBuffer<Event> ring,
            final long eventCount) {
        long published = 0L;
        while (published < eventCount) {
            final long high = ring.next(16);
            final long low = high - 15L;
            for (int index = 0; index < 16; ++index) {
                ring.get(low + index).value = published + index;
            }
            ring.publish(low, high);
            published += 16L;
        }
    }

    private static void publishBlockingP64(
            final RingBuffer<Event> ring,
            final long eventCount) {
        long published = 0L;
        while (published < eventCount) {
            final long high = ring.next(64);
            final long low = high - 63L;
            for (int index = 0; index < 64; ++index) {
                ring.get(low + index).value = published + index;
            }
            ring.publish(low, high);
            published += 64L;
        }
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
                case "--warmup-runs" ->
                        config.warmupRuns = parsePositiveInt(value, option);
                case "--producer-batch" ->
                        config.producerBatch = parsePositiveInt(value, option);
                case "--claim-policy" -> config.claimPolicy = switch (value) {
                    case "try-next" -> ClaimPolicy.TRY_NEXT;
                    case "blocking-next" -> ClaimPolicy.BLOCKING_NEXT;
                    default -> throw new IllegalArgumentException(
                            "unknown claim policy: " + value);
                };
                case "--drain-limit" ->
                        config.drainLimit = parsePositiveInt(value, option);
                case "--producer-cpu" ->
                        config.producerCpu = parseNonNegativeInt(value, option);
                case "--consumer-cpu" ->
                        config.consumerCpu = parseNonNegativeInt(value, option);
                case "--family" -> config.family = switch (value) {
                    case "strict" -> DrainFamily.STRICT;
                    case "opportunistic" -> DrainFamily.OPPORTUNISTIC;
                    default -> throw new IllegalArgumentException(
                            "unknown drain family: " + value);
                };
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

        if (config.producerBatch > RING_CAPACITY) {
            throw new IllegalArgumentException(
                    "--producer-batch must be at most 65536");
        }
        if (config.drainLimit > RING_CAPACITY) {
            throw new IllegalArgumentException(
                    "--drain-limit must be at most 65536");
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
                    "--affinity-gate is required; use run_paired.py");
        }
        if (config.events % config.producerBatch != 0L
                || config.warmupEvents % config.producerBatch != 0L) {
            throw new IllegalArgumentException(
                    "measured and warm-up event counts must be divisible by P "
                            + "so every producer claim is exactly P");
        }
        try {
            Math.addExact(
                    Math.multiplyExact(config.warmupEvents, config.warmupRuns),
                    config.events);
        } catch (final ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "warm-up plus measured positions must fit in INT64_MAX",
                    overflow);
        }
        if (config.producerBatch != 1
                && config.producerBatch != 16
                && config.producerBatch != 64) {
            throw new IllegalArgumentException(
                    "supported P values are 1, 16, and 64");
        }
        if (config.drainLimit != 1
                && config.drainLimit != 16
                && config.drainLimit != 64
                && config.drainLimit != RING_CAPACITY) {
            throw new IllegalArgumentException(
                    "supported D values are 1, 16, 64, and 65536");
        }
        if (config.family == DrainFamily.STRICT
                && config.drainLimit != config.producerBatch) {
            throw new IllegalArgumentException("strict family requires D=P");
        }
        if (config.family == DrainFamily.OPPORTUNISTIC
                && config.drainLimit != RING_CAPACITY) {
            throw new IllegalArgumentException(
                    "opportunistic family requires D=ring capacity");
        }
        return config;
    }

    private static void static_castUnused(final int ignored) {
        // Java has no static_cast<void>; this named no-op makes the initial
        // affinity verification explicit without retaining stale metadata.
    }

    private static long parsePositiveLong(
            final String value, final String option) {
        final long parsed = Long.parseLong(value);
        if (parsed <= 0L) {
            throw new IllegalArgumentException(option + " must be positive");
        }
        return parsed;
    }

    private static int parsePositiveInt(
            final String value, final String option) {
        final int parsed = Integer.parseInt(value);
        if (parsed <= 0) {
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
        System.out.println("Usage: PairedBenchmark [options]");
        System.out.println("  --events N --warmup-events N --warmup-runs N");
        System.out.println("  --producer-batch P --drain-limit D");
        System.out.println("  --claim-policy try-next|blocking-next");
        System.out.println("  --family strict|opportunistic");
        System.out.println("  --producer-cpu ID --consumer-cpu ID");
        System.out.println("  --affinity-gate PATH (managed by run_paired.py)");
        System.exit(0);
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
        // fields[0] is Linux proc field 3 (state); processor is field 39.
        if (fields.length <= 36) {
            throw new IllegalStateException("processor field missing from proc stat");
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
            final Measurement measurement,
            final long eventCount,
            final String phaseName) {
        final long expected = expectedChecksum(eventCount);
        if (measurement.durationNanos <= 0L
                || measurement.consumed != eventCount
                || measurement.checksum != expected
                || measurement.orderValueMismatch != 0L
                || measurement.producerObservedCpu != config.producerCpu
                || measurement.consumerObservedCpu != config.consumerCpu) {
            throw new IllegalStateException(phaseName + " failed validation");
        }
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
            // Emit an explicit metadata placeholder below.
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
            final Config config, final Measurement measurement) {
        final String header = String.join(",",
                "schema_version", "run_id", "sample_id", "pair_order",
                "language", "implementation", "implementation_version",
                "runtime", "build_profile", "execution_flags",
                "dependency_sha256", "git_revision", "os_name",
                "kernel_release", "cpu_model", "ring_capacity",
                "logical_event_bytes", "producer_batch",
                "producer_claim_policy", "drain_limit", "drain_family",
                "wait_strategy", "producer_cpu_requested",
                "producer_cpu_observed", "consumer_cpu_requested",
                "consumer_cpu_observed", "affinity_verified", "warmup_runs",
                "warmup_events", "events", "consumed_events", "duration_ns",
                "events_per_second", "checksum_hex", "expected_checksum_hex",
                "order_value_mismatch_hex", "valid");
        final long expected = expectedChecksum(config.events);
        final double rate = (double) config.events * 1_000_000_000.0
                / (double) measurement.durationNanos;

        final List<Object> fields = new ArrayList<>();
        fields.add("1");
        fields.add(config.runId);
        fields.add(config.sampleId);
        fields.add(config.pairOrder);
        fields.add("java");
        fields.add("LMAX Disruptor");
        fields.add(DISRUPTOR_VERSION);
        fields.add(System.getProperty("java.vm.name") + " "
                + Runtime.version());
        fields.add(config.buildProfile);
        fields.add(config.executionFlags);
        fields.add(config.dependencySha256);
        fields.add(config.gitRevision);
        fields.add("Linux");
        fields.add(kernelRelease());
        fields.add(cpuModel());
        fields.add(RING_CAPACITY);
        fields.add(8);
        fields.add(config.producerBatch);
        fields.add(config.claimPolicy.csvName);
        fields.add(config.drainLimit);
        fields.add(config.family.csvName);
        fields.add("BusySpinWaitStrategy/Thread.onSpinWait");
        fields.add(config.producerCpu);
        fields.add(measurement.producerObservedCpu);
        fields.add(config.consumerCpu);
        fields.add(measurement.consumerObservedCpu);
        fields.add(true);
        fields.add(config.warmupRuns);
        fields.add(config.warmupEvents);
        fields.add(config.events);
        fields.add(measurement.consumed);
        fields.add(measurement.durationNanos);
        fields.add(String.format(Locale.ROOT, "%.3f", rate));
        fields.add(hex64(measurement.checksum));
        fields.add(hex64(expected));
        fields.add(hex64(measurement.orderValueMismatch));
        fields.add(true);

        System.out.println(header);
        System.out.println(fields.stream()
                .map(PairedBenchmark::csvField)
                .reduce((left, right) -> left + ',' + right)
                .orElseThrow());
    }
}
