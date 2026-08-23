#!/usr/bin/env python3
"""Build and run counterbalanced same-machine C++/Java samples."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import tempfile


RING_CAPACITY = 65_536
DISRUPTOR_SHA256 = (
    "c2ba80841541272bc815bcadab910d2d716aa563eca15762450ab4c889440505"
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auditable paired C++ vs LMAX Disruptor 4.0.0 runner"
    )
    parser.add_argument("--producer-cpu", type=int, required=True)
    parser.add_argument("--consumer-cpu", type=int, required=True)
    parser.add_argument(
        "--producer-batches", type=int, nargs="+", default=[1, 16, 64]
    )
    parser.add_argument(
        "--families",
        nargs="+",
        choices=["strict", "opportunistic"],
        default=["strict", "opportunistic"],
    )
    parser.add_argument("--events", type=int, default=1_000_000_000)
    parser.add_argument("--warmup-events", type=int, default=100_000_000)
    parser.add_argument("--warmup-runs", type=int, default=2)
    parser.add_argument("--samples", type=int, default=15)
    parser.add_argument("--minimum-duration-ms", type=int, default=1_000)
    parser.add_argument("--cxx", default=os.environ.get("CXX", "g++"))
    parser.add_argument("--java", default=os.environ.get("JAVA", "java"))
    parser.add_argument(
        "--java-claim-policy",
        choices=["try-next", "blocking-next"],
        default="try-next",
        help=(
            "Java producer capacity-claim API; blocking-next is a separately "
            "labelled sensitivity, not the nonblocking contract match"
        ),
    )
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
    if any(value not in (1, 16, 64) for value in result.producer_batches):
        parser.error("supported P values are 1, 16, and 64")
    if result.events <= 0 or result.warmup_events <= 0:
        parser.error("event counts must be positive")
    if result.warmup_runs <= 0 or result.samples < 3:
        parser.error("warm-up count must be positive and samples at least 3")
    if result.minimum_duration_ms < 0:
        parser.error("--minimum-duration-ms must be non-negative")
    if len(set(result.producer_batches)) != len(result.producer_batches):
        parser.error("--producer-batches must not contain duplicates")
    if len(set(result.families)) != len(result.families):
        parser.error("--families must not contain duplicates")
    for producer_batch in result.producer_batches:
        if result.events % producer_batch != 0:
            parser.error(f"--events must be divisible by P={producer_batch}")
        if result.warmup_events % producer_batch != 0:
            parser.error(
                f"--warmup-events must be divisible by P={producer_batch}"
            )
    return result


def run_checked(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, **kwargs)


def git_revision(repository: Path) -> str:
    revision = run_checked(
        ["git", "rev-parse", "HEAD"], cwd=repository, capture_output=True
    ).stdout.strip()
    dirty = run_checked(
        ["git", "status", "--porcelain"], cwd=repository, capture_output=True
    ).stdout
    return revision + ("-dirty" if dirty else "")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_cpu_list(text: str) -> set[int]:
    output: set[int] = set()
    for part in text.strip().split(","):
        limits = part.split("-", maxsplit=1)
        first = int(limits[0])
        last = int(limits[-1])
        output.update(range(first, last + 1))
    return output


def cpu_topology(cpu: int) -> dict[str, object]:
    root = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
    return {
        "cpu": cpu,
        "package_id": int((root / "physical_package_id").read_text()),
        "core_id": int((root / "core_id").read_text()),
        "thread_siblings": sorted(
            parse_cpu_list((root / "thread_siblings_list").read_text())
        ),
    }


def validate_topology(
    producer_cpu: int, consumer_cpu: int
) -> tuple[dict[str, object], dict[str, object], list[int], list[int]]:
    allowed = os.sched_getaffinity(0)
    if not {producer_cpu, consumer_cpu}.issubset(allowed):
        raise RuntimeError(
            f"worker CPUs are outside allowed set {sorted(allowed)}"
        )
    producer = cpu_topology(producer_cpu)
    consumer = cpu_topology(consumer_cpu)
    if consumer_cpu in producer["thread_siblings"]:
        raise RuntimeError("producer and consumer CPUs are SMT siblings")
    reserved = set(producer["thread_siblings"]) | set(
        consumer["thread_siblings"]
    )
    housekeeping = allowed - reserved
    if not housekeeping:
        raise RuntimeError(
            "no housekeeping CPU remains after excluding both hot cores and "
            "all of their SMT siblings"
        )
    return producer, consumer, sorted(reserved), sorted(housekeeping)


def build(
    arguments: argparse.Namespace,
    script_dir: Path,
    repository: Path,
    build_dir: Path,
) -> tuple[Path, Path, Path, list[str], list[str]]:
    build_dir.mkdir(parents=True, exist_ok=True)
    dependencies = script_dir / ".deps"
    fetch = run_checked(
        [str(script_dir / "fetch_dependencies.sh"), str(dependencies)],
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
    cpp_binary = build_dir / "lls_disruptor_paired_cpp"
    run_checked(
        [
            arguments.cxx,
            *cxx_flags,
            "-I",
            str(
                repository
                / "modules/concurrency/disruptor-single-producer/include"
            ),
            str(script_dir / "cpp/paired_benchmark.cpp"),
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
            str(script_dir / "java/PairedBenchmark.java"),
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
        if rows[0].get("valid") != "true":
            raise RuntimeError(f"validation failed in {path}")
        return reader.fieldnames, rows[0]


def append_row(
    raw_path: Path,
    expected_header: list[str] | None,
    source_path: Path,
    minimum_duration_ns: int,
) -> tuple[list[str], dict[str, str]]:
    header, row = load_one_row(source_path)
    if expected_header is not None and header != expected_header:
        raise RuntimeError("C++ and Java CSV schemas differ")
    if int(row["duration_ns"]) < minimum_duration_ns:
        raise RuntimeError(
            f"{row['language']} {row['drain_family']} P={row['producer_batch']} "
            f"D={row['drain_limit']} sample={row['sample_id']} lasted less "
            "than --minimum-duration-ms; increase --events"
        )
    existed = raw_path.exists()
    with raw_path.open("a", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=header)
        if not existed:
            writer.writeheader()
        writer.writerow(row)
    return header, row


def t_critical_975(degrees_of_freedom: int) -> float:
    # Two-sided 95% Student-t critical values through the default 15 samples;
    # the df=30 fallback is conservative for larger audit runs.
    values = {
        1: 12.706,
        2: 4.303,
        3: 3.182,
        4: 2.776,
        5: 2.571,
        6: 2.447,
        7: 2.365,
        8: 2.306,
        9: 2.262,
        10: 2.228,
        11: 2.201,
        12: 2.179,
        13: 2.160,
        14: 2.145,
        15: 2.131,
        16: 2.120,
        17: 2.110,
        18: 2.101,
        19: 2.093,
        20: 2.086,
        21: 2.080,
        22: 2.074,
        23: 2.069,
        24: 2.064,
        25: 2.060,
        26: 2.056,
        27: 2.052,
        28: 2.048,
        29: 2.045,
        30: 2.042,
    }
    return values.get(degrees_of_freedom, 2.042)


def benchmark_arguments(
    arguments: argparse.Namespace,
    run_id: str,
    sample: int,
    order: str,
    revision: str,
    producer_batch: int,
    drain_limit: int,
    family: str,
) -> list[str]:
    return [
        "--events",
        str(arguments.events),
        "--warmup-events",
        str(arguments.warmup_events),
        "--warmup-runs",
        str(arguments.warmup_runs),
        "--producer-batch",
        str(producer_batch),
        "--drain-limit",
        str(drain_limit),
        "--family",
        family,
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

    paired_path = output_dir / "paired_ratios.csv"
    groups: dict[tuple[str, str, str, str], dict[str, dict[str, str]]] = {}
    for row in rows:
        key = (
            row["drain_family"],
            row["producer_batch"],
            row["drain_limit"],
            row["sample_id"],
        )
        groups.setdefault(key, {})[row["language"]] = row

    ratio_rows: list[dict[str, object]] = []
    for key, languages in sorted(groups.items()):
        if set(languages) != {"cpp", "java"}:
            raise RuntimeError(f"incomplete pair: {key}")
        cpp = languages["cpp"]
        java = languages["java"]
        matched_fields = (
            "run_id",
            "sample_id",
            "pair_order",
            "ring_capacity",
            "logical_event_bytes",
            "producer_batch",
            "drain_limit",
            "drain_family",
            "producer_cpu_requested",
            "consumer_cpu_requested",
            "warmup_runs",
            "warmup_events",
            "events",
        )
        for field in matched_fields:
            if cpp[field] != java[field]:
                raise RuntimeError(f"pair metadata mismatch in {field}: {key}")
        events = int(cpp["events"])
        cpp_duration = int(cpp["duration_ns"])
        java_duration = int(java["duration_ns"])
        cpp_rate = events * 1_000_000_000.0 / cpp_duration
        java_rate = events * 1_000_000_000.0 / java_duration
        ratio_rows.append(
            {
                "drain_family": key[0],
                "producer_batch": key[1],
                "drain_limit": key[2],
                "cpp_claim_policy": cpp["producer_claim_policy"],
                "java_claim_policy": java["producer_claim_policy"],
                "sample_id": key[3],
                "pair_order": languages["cpp"]["pair_order"],
                "cpp_duration_ns": cpp_duration,
                "java_duration_ns": java_duration,
                "cpp_events_per_second": f"{cpp_rate:.3f}",
                "java_events_per_second": f"{java_rate:.3f}",
                "cpp_over_java": f"{java_duration / cpp_duration:.9f}",
            }
        )
    ratio_fields = list(ratio_rows[0])
    with paired_path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=ratio_fields)
        writer.writeheader()
        writer.writerows(ratio_rows)

    summary_path = output_dir / "summary.csv"
    summaries: list[dict[str, object]] = []
    cases = sorted({(row["drain_family"], row["producer_batch"], row["drain_limit"]) for row in ratio_rows})
    for family, producer_batch, drain_limit in cases:
        selected = [
            row
            for row in ratio_rows
            if (row["drain_family"], row["producer_batch"], row["drain_limit"])
            == (family, producer_batch, drain_limit)
        ]
        cpp_claim_policies = {row["cpp_claim_policy"] for row in selected}
        java_claim_policies = {row["java_claim_policy"] for row in selected}
        if len(cpp_claim_policies) != 1 or len(java_claim_policies) != 1:
            raise RuntimeError("claim policy changed within one summary case")
        log_ratios = [math.log(float(row["cpp_over_java"])) for row in selected]
        mean_log_ratio = statistics.mean(log_ratios)
        standard_error = statistics.stdev(log_ratios) / math.sqrt(len(log_ratios))
        half_width = t_critical_975(len(log_ratios) - 1) * standard_error
        lower_ratio = math.exp(mean_log_ratio - half_width)
        upper_ratio = math.exp(mean_log_ratio + half_width)
        cpp_first_logs = [
            math.log(float(row["cpp_over_java"]))
            for row in selected
            if row["pair_order"] == "cpp-first"
        ]
        java_first_logs = [
            math.log(float(row["cpp_over_java"]))
            for row in selected
            if row["pair_order"] == "java-first"
        ]
        order_effect = math.exp(
            statistics.mean(cpp_first_logs) - statistics.mean(java_first_logs)
        )
        summaries.append(
            {
                "drain_family": family,
                "producer_batch": producer_batch,
                "drain_limit": drain_limit,
                "cpp_claim_policy": next(iter(cpp_claim_policies)),
                "java_claim_policy": next(iter(java_claim_policies)),
                "samples": len(selected),
                "median_cpp_events_per_second": f"{statistics.median(float(row['cpp_events_per_second']) for row in selected):.3f}",
                "median_java_events_per_second": f"{statistics.median(float(row['java_events_per_second']) for row in selected):.3f}",
                "median_paired_cpp_over_java": f"{statistics.median(float(row['cpp_over_java']) for row in selected):.6f}",
                "geomean_paired_cpp_over_java": f"{math.exp(mean_log_ratio):.6f}",
                "paired_ratio_95pct_ci_lower": f"{lower_ratio:.6f}",
                "paired_ratio_95pct_ci_upper": f"{upper_ratio:.6f}",
                "lower_ci_above_parity": str(lower_ratio > 1.0).lower(),
                "lower_ci_above_1_05": str(lower_ratio > 1.05).lower(),
                "cpp_first_samples": len(cpp_first_logs),
                "java_first_samples": len(java_first_logs),
                "order_effect_cpp_first_over_java_first": f"{order_effect:.6f}",
                "order_effect_within_5pct": str(
                    0.95 <= order_effect <= 1.05
                ).lower(),
            }
        )
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
    run_id = f"disruptor-paired-{stamp}"
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
    source_paths.extend(
        [repository / "CMakeLists.txt", repository / "benchmarks/CMakeLists.txt"]
    )
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

    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "utc_started": stamp,
        "repository": str(repository),
        "git_revision": revision,
        "producer_cpu": arguments.producer_cpu,
        "consumer_cpu": arguments.consumer_cpu,
        "producer_topology": producer_topology,
        "consumer_topology": consumer_topology,
        "reserved_hot_core_cpus": reserved_cpus,
        "helper_housekeeping_cpus": housekeeping_cpus,
        "producer_batches": arguments.producer_batches,
        "families": arguments.families,
        "events": arguments.events,
        "warmup_events": arguments.warmup_events,
        "warmup_runs": arguments.warmup_runs,
        "samples": arguments.samples,
        "minimum_duration_ms": arguments.minimum_duration_ms,
        "cxx": arguments.cxx,
        "cxx_version": run_checked(
            [arguments.cxx, "--version"], capture_output=True
        ).stdout.splitlines()[0],
        "cxx_flags": cxx_flags,
        "java": arguments.java,
        "java_claim_policy": arguments.java_claim_policy,
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
        "counterbalance": "odd pairs C++ first; even pairs Java first",
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    raw_path = output_dir / "raw.csv"
    header: list[str] | None = None
    pair_number = 0
    minimum_duration_ns = arguments.minimum_duration_ms * 1_000_000
    with tempfile.TemporaryDirectory(prefix="lls-paired-") as temporary_text:
        temporary = Path(temporary_text)
        for producer_batch in arguments.producer_batches:
            for family in arguments.families:
                drain_limit = (
                    producer_batch if family == "strict" else RING_CAPACITY
                )
                for sample in range(1, arguments.samples + 1):
                    pair_number += 1
                    order = "cpp-first" if pair_number % 2 == 1 else "java-first"
                    common = benchmark_arguments(
                        arguments,
                        run_id,
                        sample,
                        order,
                        revision,
                        producer_batch,
                        drain_limit,
                        family,
                    )
                    cpp_output = temporary / f"cpp-{pair_number}.csv"
                    java_output = temporary / f"java-{pair_number}.csv"
                    gate = temporary / f"java-{pair_number}.gate"
                    cpp_command = [
                        str(cpp_binary),
                        *common,
                        "--claim-policy",
                        "try-publish-batch",
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
                        "--",
                        arguments.java,
                        *java_flags,
                        "-cp",
                        f"{classes}{os.pathsep}{jar}",
                        "PairedBenchmark",
                        *common,
                        "--claim-policy",
                        arguments.java_claim_policy,
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
                        header, _ = append_row(
                            raw_path,
                            header,
                            output,
                            minimum_duration_ns,
                        )

    write_summaries(raw_path, output_dir)
    print(output_dir)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"paired runner: {error}", file=sys.stderr)
        raise SystemExit(1)
