#!/usr/bin/env python3
"""Build and run the paired C++ / LMAX handoff-latency comparator."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import tempfile

from run_paired import (
    DISRUPTOR_SHA256,
    git_revision,
    run_checked,
    sha256_file,
    t_critical_975,
    validate_topology,
)


RING_CAPACITY = 65_536
MINIMUM_WARMUP_EVENTS = 100_000
DEFAULT_WARMUP_EVENTS = 1_000_000
MINIMUM_MEASURED_EVENTS = 1_000_000
METRICS = ("p50_ns", "p90_ns", "p95_ns", "p99_ns", "p99_9_ns", "max_ns")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auditable paired C++ vs LMAX handoff-latency runner"
    )
    parser.add_argument("--producer-cpu", type=int, required=True)
    parser.add_argument("--consumer-cpu", type=int, required=True)
    parser.add_argument("--events", type=int, default=MINIMUM_MEASURED_EVENTS)
    parser.add_argument(
        "--warmup-events", type=int, default=DEFAULT_WARMUP_EVENTS
    )
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--cxx", default=os.environ.get("CXX", "g++"))
    parser.add_argument("--java", default=os.environ.get("JAVA", "java"))
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--jvm-arg",
        action="append",
        default=[],
        help="replace default JVM arguments; repeat once per argument",
    )
    result = parser.parse_args()
    if result.producer_cpu == result.consumer_cpu:
        parser.error("producer and consumer CPUs must differ")
    if result.warmup_events < MINIMUM_WARMUP_EVENTS:
        parser.error(
            f"--warmup-events must be at least {MINIMUM_WARMUP_EVENTS}"
        )
    if result.events < MINIMUM_MEASURED_EVENTS:
        parser.error(f"--events must be at least {MINIMUM_MEASURED_EVENTS}")
    if result.warmup_events + result.events > 2_147_483_647:
        parser.error("warm-up plus measured samples must fit in a Java array")
    if result.samples < 7:
        parser.error("--samples must be at least 7 for the audited comparison")
    return result


def build(
    arguments: argparse.Namespace,
    script_dir: Path,
    repository: Path,
    build_dir: Path,
) -> tuple[Path, Path, Path, list[str], list[str]]:
    build_dir.mkdir(parents=True, exist_ok=True)
    fetch = run_checked(
        [
            str(script_dir / "fetch_dependencies.sh"),
            str(script_dir / ".deps"),
        ],
        capture_output=True,
    )
    jar = Path(fetch.stdout.strip())

    cxx_flags = [
        "-std=c++23",
        "-O3",
        "-DNDEBUG",
        "-march=native",
        "-mtune=native",
        "-flto",
        "-pthread",
        "-Wall",
        "-Wextra",
        "-Wpedantic",
        "-Wconversion",
        "-Wsign-conversion",
        "-Wshadow",
        "-Werror",
    ]
    cpp_binary = build_dir / "lls_disruptor_paired_latency_cpp"
    run_checked(
        [
            arguments.cxx,
            *cxx_flags,
            "-I",
            str(
                repository
                / "modules/concurrency/disruptor-single-producer/include"
            ),
            str(script_dir / "cpp/paired_latency_benchmark.cpp"),
            "-o",
            str(cpp_binary),
        ]
    )

    classes = build_dir / "java-classes"
    classes.mkdir(parents=True, exist_ok=True)
    compiler = shutil.which("javac")
    compiler_prefix = (
        [compiler]
        if compiler is not None
        else [arguments.java, "-m", "jdk.compiler/com.sun.tools.javac.Main"]
    )
    run_checked(
        [
            *compiler_prefix,
            "--release",
            "17",
            "-Xlint:all",
            "-Werror",
            "-cp",
            str(jar),
            "-d",
            str(classes),
            str(script_dir / "java/PairedLatencyBenchmark.java"),
        ]
    )
    java_flags = arguments.jvm_arg or [
        "-server",
        "-Xms1g",
        "-Xmx1g",
        "-XX:+UseSerialGC",
    ]
    return cpp_binary, classes, jar, cxx_flags, java_flags


def load_one_row(path: Path) -> tuple[list[str], dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        rows = list(reader)
        if reader.fieldnames is None or len(rows) != 1:
            raise RuntimeError(f"expected one CSV row from {path}")
        row = rows[0]
        if row.get("valid") != "true":
            raise RuntimeError(f"validation failed in {path}")
        values = [int(row[metric]) for metric in METRICS]
        if any(value <= 0 for value in values):
            raise RuntimeError(f"non-positive percentile in {path}")
        if values != sorted(values):
            raise RuntimeError(f"non-monotonic percentile distribution in {path}")
        total = int(row["warmup_events"]) + int(row["events"])
        if int(row["consumed_events"]) != total:
            raise RuntimeError(f"count validation failed in {path}")
        if int(row["positive_measured_events"]) != int(row["events"]):
            raise RuntimeError(f"positive-latency validation failed in {path}")
        if row["sequence_checksum_hex"] != row[
            "expected_sequence_checksum_hex"
        ]:
            raise RuntimeError(f"sequence checksum failed in {path}")
        if int(row["order_mismatch_hex"], 16) != 0:
            raise RuntimeError(f"sequence order failed in {path}")
        return reader.fieldnames, row


def append_row(
    raw_path: Path,
    expected_header: list[str] | None,
    source_path: Path,
) -> tuple[list[str], dict[str, str]]:
    header, row = load_one_row(source_path)
    if expected_header is not None and header != expected_header:
        raise RuntimeError("C++ and Java CSV schemas differ")
    existed = raw_path.exists()
    with raw_path.open("a", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=header)
        if not existed:
            writer.writeheader()
        writer.writerow(row)
    return header, row


def benchmark_arguments(
    arguments: argparse.Namespace,
    run_id: str,
    sample: int,
    order: str,
    revision: str,
) -> list[str]:
    return [
        "--events",
        str(arguments.events),
        "--warmup-events",
        str(arguments.warmup_events),
        "--producer-cpu",
        str(arguments.producer_cpu),
        "--consumer-cpu",
        str(arguments.consumer_cpu),
        "--run-id",
        run_id,
        "--sample-id",
        str(sample),
        "--pair-order",
        order,
        "--git-revision",
        revision,
    ]


def write_summaries(raw_path: Path, output_dir: Path) -> None:
    with raw_path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))

    pairs: dict[str, dict[str, dict[str, str]]] = {}
    for row in rows:
        pairs.setdefault(row["sample_id"], {})[row["language"]] = row

    ratio_rows: list[dict[str, object]] = []
    for sample_id, languages in sorted(
        pairs.items(), key=lambda item: int(item[0])
    ):
        if set(languages) != {"cpp", "java"}:
            raise RuntimeError(f"incomplete pair: {sample_id}")
        cpp = languages["cpp"]
        java = languages["java"]
        matched_fields = (
            "benchmark",
            "run_id",
            "sample_id",
            "pair_order",
            "ring_capacity",
            "logical_event_bytes",
            "producer_batch",
            "drain_limit",
            "drain_family",
            "wait_strategy",
            "latency_boundary",
            "queue_residence_policy",
            "producer_cpu_requested",
            "consumer_cpu_requested",
            "warmup_events",
            "events",
        )
        for field in matched_fields:
            if cpp[field] != java[field]:
                raise RuntimeError(
                    f"pair metadata mismatch in {field}: sample {sample_id}"
                )
        if cpp["producer_claim_policy"] != "try-publish-batch":
            raise RuntimeError("unexpected C++ capacity-claim policy")
        if java["producer_claim_policy"] != "try-next":
            raise RuntimeError("unexpected Java capacity-claim policy")
        output: dict[str, object] = {
            "sample_id": sample_id,
            "pair_order": cpp["pair_order"],
            "events": cpp["events"],
            "warmup_events": cpp["warmup_events"],
        }
        for metric in METRICS:
            cpp_value = int(cpp[metric])
            java_value = int(java[metric])
            output[f"cpp_{metric}"] = cpp_value
            output[f"java_{metric}"] = java_value
            output[f"cpp_over_java_{metric}"] = f"{cpp_value / java_value:.9f}"
        ratio_rows.append(output)

    paired_path = output_dir / "paired_ratios.csv"
    ratio_fields = list(ratio_rows[0])
    with paired_path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=ratio_fields)
        writer.writeheader()
        writer.writerows(ratio_rows)

    summaries: list[dict[str, object]] = []
    for metric in METRICS:
        ratios = [float(row[f"cpp_over_java_{metric}"]) for row in ratio_rows]
        logs = [math.log(value) for value in ratios]
        mean_log = statistics.mean(logs)
        standard_error = statistics.stdev(logs) / math.sqrt(len(logs))
        half_width = t_critical_975(len(logs) - 1) * standard_error
        cpp_first = [
            math.log(float(row[f"cpp_over_java_{metric}"]))
            for row in ratio_rows
            if row["pair_order"] == "cpp-first"
        ]
        java_first = [
            math.log(float(row[f"cpp_over_java_{metric}"]))
            for row in ratio_rows
            if row["pair_order"] == "java-first"
        ]
        order_effect = math.exp(
            statistics.mean(cpp_first) - statistics.mean(java_first)
        )
        lower = math.exp(mean_log - half_width)
        upper = math.exp(mean_log + half_width)
        summaries.append(
            {
                "metric": metric,
                "samples": len(ratio_rows),
                "median_cpp_ns": f"{statistics.median(int(row[f'cpp_{metric}']) for row in ratio_rows):.0f}",
                "median_java_ns": f"{statistics.median(int(row[f'java_{metric}']) for row in ratio_rows):.0f}",
                "median_paired_cpp_over_java": f"{statistics.median(ratios):.6f}",
                "geomean_paired_cpp_over_java": f"{math.exp(mean_log):.6f}",
                "paired_ratio_95pct_ci_lower": f"{lower:.6f}",
                "paired_ratio_95pct_ci_upper": f"{upper:.6f}",
                "upper_ci_below_parity": str(upper < 1.0).lower(),
                "upper_ci_below_0_95": str(upper < 0.95).lower(),
                "cpp_first_samples": len(cpp_first),
                "java_first_samples": len(java_first),
                "order_effect_cpp_first_over_java_first": f"{order_effect:.6f}",
                "order_effect_within_5pct": str(
                    0.95 <= order_effect <= 1.05
                ).lower(),
            }
        )
    summary_path = output_dir / "summary.csv"
    summary_fields = list(summaries[0])
    with summary_path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summaries)


def main() -> int:
    arguments = parse_arguments()
    script_dir = Path(__file__).resolve().parent
    repository = script_dir.parents[2]
    producer_topology, consumer_topology, reserved_cpus, housekeeping_cpus = (
        validate_topology(arguments.producer_cpu, arguments.consumer_cpu)
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"disruptor-paired-latency-{stamp}"
    output_dir = arguments.output_dir or script_dir / "results" / run_id
    output_dir.mkdir(parents=True, exist_ok=False)
    build_dir = output_dir / "build"
    revision = git_revision(repository)
    cpp_binary, classes, jar, cxx_flags, java_flags = build(
        arguments, script_dir, repository, build_dir
    )

    source_paths = [
        path
        for path in script_dir.rglob("*")
        if path.is_file()
        and (
            path.name == "CMakeLists.txt"
            or path.suffix in {".cpp", ".java", ".py", ".sh", ".lock"}
        )
        and not any(
            component in {".deps", "build", "results", "__pycache__"}
            for component in path.parts
        )
    ]
    production_header = repository / (
        "modules/concurrency/disruptor-single-producer/include/lls/"
        "concurrency/disruptor_single_producer.hpp"
    )
    source_paths.append(production_header)
    source_hashes = {
        str(path.relative_to(repository)): sha256_file(path)
        for path in sorted(set(source_paths))
    }
    class_hashes = {
        str(path.relative_to(classes)): sha256_file(path)
        for path in sorted(classes.rglob("*.class"))
    }
    status = run_checked(
        ["git", "status", "--porcelain=v1"],
        cwd=repository,
        capture_output=True,
    ).stdout.splitlines()
    patch_path = output_dir / "tracked-source.patch"
    with patch_path.open("wb") as patch_output:
        subprocess.run(
            ["git", "diff", "--binary", "HEAD", "--"],
            cwd=repository,
            check=True,
            stdout=patch_output,
        )
    try:
        clocksource = Path(
            "/sys/devices/system/clocksource/clocksource0/current_clocksource"
        ).read_text().strip()
    except OSError:
        clocksource = "unknown"

    manifest = {
        "schema_version": 1,
        "benchmark": "paired-handoff-latency",
        "run_id": run_id,
        "utc_started": stamp,
        "repository": str(repository),
        "git_revision": revision,
        "contract": {
            "ring_capacity": RING_CAPACITY,
            "logical_event_bytes": 8,
            "producer_batch": 1,
            "drain_limit": 1,
            "drain_family": "strict",
            "cpp_claim_policy": "try-publish-batch",
            "java_claim_policy": "try-next",
            "claim_timestamp_order": "claim, timestamp, publish",
            "latency_boundary": (
                "post-claim producer timestamp to consumer handler-entry "
                "timestamp"
            ),
            "queue_residence_policy": (
                "producer acquire-waits for consumer release after every event"
            ),
            "percentile_index": "floor((N - 1) * percentile)",
            "metrics": list(METRICS),
            "paired_ratio": (
                "C++ latency / Java latency; lower than 1 means C++ is lower"
            ),
            "order_effect": (
                "geomean C++/Java ratio in C++-first pairs divided by the "
                "geomean ratio in Java-first pairs; 1 means no order effect"
            ),
        },
        "producer_cpu": arguments.producer_cpu,
        "consumer_cpu": arguments.consumer_cpu,
        "producer_topology": producer_topology,
        "consumer_topology": consumer_topology,
        "reserved_hot_core_cpus": reserved_cpus,
        "helper_housekeeping_cpus": housekeeping_cpus,
        "events": arguments.events,
        "warmup_events": arguments.warmup_events,
        "samples": arguments.samples,
        "cxx": arguments.cxx,
        "cxx_version": run_checked(
            [arguments.cxx, "--version"], capture_output=True
        ).stdout.splitlines()[0],
        "cxx_flags": cxx_flags,
        "java": arguments.java,
        "java_version": run_checked(
            [arguments.java, "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ).stdout.splitlines()[0],
        "jvm_flags": java_flags,
        "disruptor_jar": str(jar),
        "disruptor_sha256": DISRUPTOR_SHA256,
        "built_cpp_sha256": sha256_file(cpp_binary),
        "compiled_class_sha256": class_hashes,
        "source_sha256": source_hashes,
        "git_status_porcelain": status,
        "tracked_source_patch": patch_path.name,
        "tracked_source_patch_sha256": sha256_file(patch_path),
        "allowed_cpus": sorted(os.sched_getaffinity(0)),
        "kernel_clocksource": clocksource,
        "counterbalance": "odd pairs C++ first; even pairs Java first",
        "acceptance": (
            "configuration, dependency, affinity, count, order, checksum, "
            "and all-positive latency only; performance is not a pass gate"
        ),
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    raw_path = output_dir / "raw.csv"
    header: list[str] | None = None
    with tempfile.TemporaryDirectory(prefix="lls-paired-latency-") as text:
        temporary = Path(text)
        for sample in range(1, arguments.samples + 1):
            order = "cpp-first" if sample % 2 == 1 else "java-first"
            common = benchmark_arguments(
                arguments, run_id, sample, order, revision
            )
            cpp_output = temporary / f"cpp-{sample}.csv"
            java_output = temporary / f"java-{sample}.csv"
            gate = temporary / f"java-{sample}.gate"
            cpp_command = [
                str(cpp_binary),
                *common,
                "--build-profile",
                "O3-NDEBUG-native-LTO",
                "--execution-flags",
                " ".join(cxx_flags),
            ]
            java_command = [
                sys.executable,
                str(script_dir / "run_java_pinned.py"),
                "--producer-cpu",
                str(arguments.producer_cpu),
                "--consumer-cpu",
                str(arguments.consumer_cpu),
                "--gate",
                str(gate),
                "--stdout",
                str(java_output),
                "--main-class",
                "PairedLatencyBenchmark",
                "--",
                arguments.java,
                *java_flags,
                "-cp",
                f"{classes}{os.pathsep}{jar}",
                "PairedLatencyBenchmark",
                *common,
                "--build-profile",
                "server-JIT",
                "--execution-flags",
                " ".join(java_flags),
                "--dependency-sha256",
                DISRUPTOR_SHA256,
                "--affinity-gate",
                str(gate),
            ]
            commands = (
                [(cpp_command, cpp_output), (java_command, java_output)]
                if order == "cpp-first"
                else [(java_command, java_output), (cpp_command, cpp_output)]
            )
            for command, output in commands:
                if output == cpp_output:
                    with output.open(
                        "w", encoding="utf-8", newline=""
                    ) as destination:
                        run_checked(command, stdout=destination)
                else:
                    run_checked(command)
                header, _ = append_row(raw_path, header, output)

    write_summaries(raw_path, output_dir)
    print(output_dir)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print(f"paired latency runner: {error}", file=sys.stderr)
        raise SystemExit(1)
