#!/usr/bin/env python3

import argparse
import csv
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


PREFIX_COLUMNS = (
    "macro_run",
    "tournament_position",
    "cgroup_throttled_periods",
    "cgroup_throttled_usec",
)

PROFILE_EXPECTATIONS = {
    # protocols, spin, nodelay, connected, busy-poll, pause interval,
    # UDP control-check interval, UDP receive API
    "baseline": (("tcp", "udp"), False, False, False, 0, 0, 0, "recvmsg"),
    "tcp-nodelay": (("tcp",), False, True, False, 0, 0, 0, "recvmsg"),
    "tcp-spin": (("tcp",), True, False, False, 0, 1, 0, "recvmsg"),
    "tcp-spin-pause4": (("tcp",), True, False, False, 0, 4, 0, "recvmsg"),
    "tcp-spin-unpaused": (("tcp",), True, False, False, 0, 0, 0, "recvmsg"),
    "tcp-nodelay-spin": (("tcp",), True, True, False, 0, 1, 0, "recvmsg"),
    "tcp-quickack": (("tcp",), False, False, False, 0, 0, 0, "recvmsg"),
    "tcp-quickack-spin": (("tcp",), True, False, False, 0, 1, 0, "recvmsg"),
    "tcp-nodelay-quickack": (("tcp",), False, True, False, 0, 0, 0, "recvmsg"),
    "tcp-nodelay-quickack-spin": (("tcp",), True, True, False, 0, 1, 0, "recvmsg"),
    "udp-connected": (("udp",), False, False, True, 0, 0, 0, "recvmsg"),
    "udp-spin": (("udp",), True, False, False, 0, 1, 1, "recvmsg"),
    "udp-connected-spin": (("udp",), True, False, True, 0, 1, 1, "recvmsg"),
    "udp-connected-spin-pause4": (("udp",), True, False, True, 0, 4, 1, "recvmsg"),
    "udp-connected-spin-unpaused": (("udp",), True, False, True, 0, 0, 1, "recvmsg"),
    "udp-connected-recv-spin": (("udp",), True, False, True, 0, 1, 1, "recv"),
    "udp-connected-peerless-recvmsg-spin": (
        ("udp",), True, False, True, 0, 1, 1, "recvmsg-no-peer"
    ),
    "udp-connected-peerless-recvmsg-spin-pause4": (
        ("udp",), True, False, True, 0, 4, 1, "recvmsg-no-peer"
    ),
    "udp-connected-peerless-recvmsg-spin-unpaused": (
        ("udp",), True, False, True, 0, 0, 1, "recvmsg-no-peer"
    ),
    "udp-connected-recv-spin-pause4": (("udp",), True, False, True, 0, 4, 1, "recv"),
    "udp-connected-recv-spin-unpaused": (("udp",), True, False, True, 0, 0, 1, "recv"),
    "udp-connected-spin-check64": (("udp",), True, False, True, 0, 1, 64, "recvmsg"),
    "udp-connected-recv-spin-check64": (("udp",), True, False, True, 0, 1, 64, "recv"),
    "busy-poll-50": (("tcp", "udp"), False, False, False, 50, 0, 0, "recvmsg"),
    "tcp-nodelay-busy-poll-50": (("tcp",), False, True, False, 50, 0, 0, "recvmsg"),
    "udp-connected-busy-poll-50": (("udp",), False, False, True, 50, 0, 0, "recvmsg"),
}

QUICKACK_PROFILES = {
    "tcp-quickack",
    "tcp-quickack-spin",
    "tcp-nodelay-quickack",
    "tcp-nodelay-quickack-spin",
}


def positive_integer(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run balanced, interleaved socket-profile comparisons"
    )
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--protocol", required=True, choices=("tcp", "udp"))
    parser.add_argument("--payloads", required=True, nargs="+", type=int)
    parser.add_argument("--profiles", required=True, nargs="+")
    parser.add_argument("--reference-profile", default="baseline")
    parser.add_argument("--observations", type=positive_integer)
    parser.add_argument("--warmup", type=positive_integer, default=10_000)
    parser.add_argument("--samples", type=positive_integer, default=50_000)
    parser.add_argument("--client-cpu", type=int, required=True)
    parser.add_argument("--server-cpu", type=int, required=True)
    parser.add_argument("--udp-timeout-ms", type=positive_integer, default=100)
    parser.add_argument("--timeout-seconds", type=positive_integer, default=300)
    arguments = parser.parse_args()
    if len(set(arguments.profiles)) != len(arguments.profiles):
        parser.error("profiles must be unique")
    if arguments.reference_profile not in arguments.profiles:
        parser.error("profiles must include --reference-profile")
    unknown_profiles = sorted(
        set(arguments.profiles) - set(PROFILE_EXPECTATIONS)
    )
    if unknown_profiles:
        parser.error("unknown profiles: " + ", ".join(unknown_profiles))
    incompatible_profiles = [
        profile
        for profile in arguments.profiles
        if arguments.protocol not in PROFILE_EXPECTATIONS[profile][0]
    ]
    if incompatible_profiles:
        parser.error(
            f"profiles do not support {arguments.protocol}: "
            + ", ".join(incompatible_profiles)
        )
    if any(payload not in (8, 64, 256, 1400) for payload in arguments.payloads):
        parser.error("payloads must be chosen from 8, 64, 256, and 1400")
    if len(set(arguments.payloads)) != len(arguments.payloads):
        parser.error("payloads must be unique")
    if arguments.client_cpu < 0 or arguments.server_cpu < 0:
        parser.error("CPU indices must be non-negative")
    if arguments.client_cpu == arguments.server_cpu:
        parser.error("client and server CPUs must differ")
    width = len(arguments.profiles) * len(arguments.payloads)
    if width > 1 and width % 2 != 0:
        parser.error("profile/payload condition count must be one or even")
    if arguments.observations is None:
        arguments.observations = width
    if arguments.observations % width != 0:
        parser.error("observations must be a multiple of condition count")
    return arguments


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def williams_first_row(width):
    if width == 1:
        return [0]
    if width == 0 or width % 2 != 0:
        raise ValueError("Williams ordering requires one or an even width")
    order = []
    for position in range(width):
        if position == 0:
            order.append(0)
        elif position % 2:
            order.append((position + 1) // 2)
        else:
            order.append(width - position // 2)
    return order


def read_cpu_stat():
    path = Path("/sys/fs/cgroup/cpu.stat")
    if not path.is_file():
        return {}
    values = {}
    with path.open(encoding="utf-8") as source:
        for line in source:
            fields = line.split()
            if len(fields) == 2:
                values[fields[0]] = int(fields[1])
    return values


def stat_delta(before, after, key):
    return max(0, after.get(key, 0) - before.get(key, 0))


def parse_single_row(stdout):
    lines = stdout.splitlines()
    if len(lines) != 2:
        raise ValueError(f"benchmark emitted {len(lines)} lines instead of two")
    reader = csv.DictReader(lines)
    row = next(reader)
    if next(reader, None) is not None:
        raise ValueError("benchmark emitted more than one result row")
    return list(reader.fieldnames or []), row


def validate_row(row, profile, protocol, payload, samples, warmup, deadline):
    required_columns = {
        "profile",
        "protocol",
        "receive_wait",
        "udp_connected",
        "spin_pause_interval",
        "udp_spin_control_check_interval",
        "udp_receive_api",
        "tcp_nodelay_requested",
        "tcp_quickack_rearm",
        "busy_poll_requested_us",
        "client_busy_poll_us",
        "server_busy_poll_us",
        "payload_bytes",
        "run",
        "workload_position",
        "successful_samples",
        "attempted_samples",
        "warmup",
        "udp_deadline_ms",
        "min_ns",
        "p50_ns",
        "p90_ns",
        "p95_ns",
        "p99_ns",
        "p999_ns",
        "max_ns",
        "validated_round_trips_per_second",
        "application_messages_per_second",
        "client_cpu_percent",
        "server_cpu_percent",
        "process_cpu_percent",
        "client_cpu_ns_per_attempt",
        "server_cpu_ns_per_attempt",
        "tcp_connect_ns",
        "client_send_buffer_bytes",
        "client_receive_buffer_bytes",
        "server_send_buffer_bytes",
        "server_receive_buffer_bytes",
        "client_tcp_nodelay",
        "server_tcp_nodelay",
        "udp_deadline_misses",
        "udp_requests_unobserved",
        "udp_response_deadline_misses",
        "udp_combined_duplicate_events",
        "udp_combined_reordered_events",
        "udp_combined_invalid_events",
        "udp_combined_truncated_events",
        "checksum",
        "validation",
    }
    missing = sorted(required_columns - set(row))
    if missing:
        raise ValueError("benchmark row is missing: " + ", ".join(missing))
    if row["validation"] != "PASS":
        raise ValueError("benchmark row did not pass validation")
    expected_strings = {
        "profile": profile,
        "protocol": protocol,
        "payload_bytes": str(payload),
        "attempted_samples": str(samples),
        "warmup": str(warmup),
        "udp_deadline_ms": str(deadline),
        "run": "1",
        "workload_position": "1",
    }
    for column, expected in expected_strings.items():
        if row[column] != expected:
            raise ValueError(
                f"unexpected {column}: {row[column]!r}, expected {expected!r}"
            )

    successful = int(row["successful_samples"])
    attempted = int(row["attempted_samples"])
    deadline_misses = int(row["udp_deadline_misses"])
    unobserved = int(row["udp_requests_unobserved"])
    response_misses = int(row["udp_response_deadline_misses"])
    if successful + deadline_misses != attempted:
        raise ValueError("successful/deadline sample accounting is inconsistent")
    if successful <= 0 or successful > attempted:
        raise ValueError("successful sample count is outside its valid range")
    if unobserved + response_misses != deadline_misses:
        raise ValueError("UDP deadline split is inconsistent")
    anomaly_columns = (
        "udp_requests_unobserved",
        "udp_combined_duplicate_events",
        "udp_combined_reordered_events",
        "udp_combined_invalid_events",
        "udp_combined_truncated_events",
    )
    if any(int(row[column]) != 0 for column in anomaly_columns):
        raise ValueError("profile produced a UDP correctness anomaly")
    if protocol == "tcp" and deadline_misses != 0:
        raise ValueError("TCP profile did not complete every exchange")

    (
        _,
        spin,
        expected_nodelay,
        expected_connected,
        expected_busy_poll,
        expected_pause_interval,
        expected_control_interval,
        expected_udp_receive_api,
    ) = PROFILE_EXPECTATIONS[profile]
    expected_profile_fields = {
        "receive_wait": "spin" if spin else (
            "blocking" if protocol == "tcp" else "poll"
        ),
        "udp_connected": str(int(expected_connected)),
        "spin_pause_interval": str(expected_pause_interval),
        "udp_spin_control_check_interval": str(
            expected_control_interval if protocol == "udp" else 0
        ),
        "udp_receive_api": (
            expected_udp_receive_api if protocol == "udp" else "none"
        ),
        "tcp_nodelay_requested": str(int(expected_nodelay)),
        "tcp_quickack_rearm": str(int(profile in QUICKACK_PROFILES)),
        "busy_poll_requested_us": str(expected_busy_poll),
    }
    for column, expected in expected_profile_fields.items():
        if row[column] != expected:
            raise ValueError(
                f"unexpected {column}: {row[column]!r}, expected {expected!r}"
            )

    requested_nodelay = int(row["tcp_nodelay_requested"])
    if protocol == "tcp":
        if int(row["client_tcp_nodelay"]) != requested_nodelay:
            raise ValueError("client TCP_NODELAY does not match request")
        if int(row["server_tcp_nodelay"]) != requested_nodelay:
            raise ValueError("server TCP_NODELAY does not match request")
    elif (
        int(row["client_tcp_nodelay"]) != -1
        or int(row["server_tcp_nodelay"]) != -1
    ):
        raise ValueError("UDP row has TCP_NODELAY state")

    requested_busy_poll = int(row["busy_poll_requested_us"])
    if requested_busy_poll > 0:
        if int(row["client_busy_poll_us"]) != requested_busy_poll:
            raise ValueError("client SO_BUSY_POLL does not match request")
        if int(row["server_busy_poll_us"]) != requested_busy_poll:
            raise ValueError("server SO_BUSY_POLL does not match request")

    percentiles = [
        int(row[column])
        for column in (
            "min_ns",
            "p50_ns",
            "p90_ns",
            "p95_ns",
            "p99_ns",
            "p999_ns",
            "max_ns",
        )
    ]
    if percentiles[0] <= 0 or any(
        left > right for left, right in zip(percentiles, percentiles[1:])
    ):
        raise ValueError("latency statistics are not positive and monotonic")
    if int(row["checksum"]) <= 0:
        raise ValueError("profile emitted an invalid latency or checksum")

    rate_columns = (
        "validated_round_trips_per_second",
        "application_messages_per_second",
        "client_cpu_percent",
        "server_cpu_percent",
        "process_cpu_percent",
        "client_cpu_ns_per_attempt",
        "server_cpu_ns_per_attempt",
    )
    values = {column: float(row[column]) for column in rate_columns}
    if any(not math.isfinite(value) or value < 0 for value in values.values()):
        raise ValueError("rate or CPU metric is invalid")
    if values["validated_round_trips_per_second"] <= 0:
        raise ValueError("validated rate must be positive")
    expected_message_rate = 2.0 * values["validated_round_trips_per_second"]
    if not math.isclose(
        values["application_messages_per_second"],
        expected_message_rate,
        rel_tol=1e-6,
        abs_tol=0.002,
    ):
        raise ValueError("application message rate is inconsistent")

    buffer_columns = (
        "client_send_buffer_bytes",
        "client_receive_buffer_bytes",
        "server_send_buffer_bytes",
        "server_receive_buffer_bytes",
    )
    if any(int(row[column]) <= 0 for column in buffer_columns):
        raise ValueError("socket buffer observation is invalid")
    connect_ns = int(row["tcp_connect_ns"])
    if (protocol == "tcp" and connect_ns <= 0) or (
        protocol == "udp" and connect_ns != 0
    ):
        raise ValueError("TCP connect diagnostic is inconsistent")


def run_condition(arguments, profile, payload):
    command = [
        str(arguments.binary),
        "--warmup",
        str(arguments.warmup),
        "--samples",
        str(arguments.samples),
        "--runs",
        "1",
        "--client-cpu",
        str(arguments.client_cpu),
        "--server-cpu",
        str(arguments.server_cpu),
        "--udp-timeout-ms",
        str(arguments.udp_timeout_ms),
        "--profile",
        profile,
        "--protocol",
        arguments.protocol,
        "--payload",
        str(payload),
    ]
    before = read_cpu_stat()
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=arguments.timeout_seconds,
    )
    after = read_cpu_stat()
    if completed.returncode != 0:
        raise RuntimeError(
            f"profile {profile}/{payload} failed with {completed.returncode}: "
            f"{completed.stderr.strip()}"
        )
    fieldnames, row = parse_single_row(completed.stdout)
    validate_row(
        row,
        profile,
        arguments.protocol,
        payload,
        arguments.samples,
        arguments.warmup,
        arguments.udp_timeout_ms,
    )
    diagnostics = {
        "cgroup_throttled_periods": stat_delta(
            before, after, "nr_throttled"
        ),
        "cgroup_throttled_usec": stat_delta(
            before, after, "throttled_usec"
        ),
    }
    return command, completed.stderr.strip(), fieldnames, row, diagnostics


def numeric_median(rows, column):
    return statistics.median(float(row[column]) for row in rows)


def validate_tournament(rows, profile_order, payloads, observations):
    conditions = [
        (profile, payload)
        for payload in payloads
        for profile in profile_order
    ]
    width = len(conditions)
    if len(rows) != width * observations:
        raise ValueError("tournament row count is incomplete")
    first_row = williams_first_row(width)
    groups = defaultdict(list)

    for macro_run in range(observations):
        macro_rows = [
            row for row in rows if int(row["macro_run"]) == macro_run + 1
        ]
        if len(macro_rows) != width:
            raise ValueError("macro-run row count is incomplete")
        by_position = {
            int(row["tournament_position"]): row for row in macro_rows
        }
        if sorted(by_position) != list(range(1, width + 1)):
            raise ValueError("tournament positions are incomplete or repeated")
        for position, base_index in enumerate(first_row):
            expected = conditions[(base_index + macro_run) % width]
            row = by_position[position + 1]
            observed = (row["profile"], int(row["payload_bytes"]))
            if observed != expected:
                raise ValueError("tournament order does not match its design")
            groups[observed].append(row)

    expected_position_count = observations // width
    for condition in conditions:
        group = groups[condition]
        if len(group) != observations:
            raise ValueError("condition observation count is incomplete")
        position_counts = defaultdict(int)
        for row in group:
            position_counts[int(row["tournament_position"])] += 1
        if any(
            position_counts[position] != expected_position_count
            for position in range(1, width + 1)
        ):
            raise ValueError("condition position balance is incomplete")

        checksums = {row["checksum"] for row in group}
        zero_deadline_misses = all(
            int(row["udp_deadline_misses"]) == 0 for row in group
        )
        if (group[0]["protocol"] == "tcp" or zero_deadline_misses) and (
            len(checksums) != 1
        ):
            raise ValueError("stable treatment produced inconsistent checksums")


def format_number(value):
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.6f}"


def write_summary(path, rows, profile_order, payloads, reference_profile):
    by_group = defaultdict(list)
    by_macro = {}
    for row in rows:
        key = (row["profile"], int(row["payload_bytes"]))
        by_group[key].append(row)
        by_macro[(
            int(row["macro_run"]),
            row["profile"],
            int(row["payload_bytes"]),
        )] = row

    header = [
        "profile",
        "payload_bytes",
        "observations",
        "p50_wins_vs_reference",
        "min_run_p50_ns",
        "median_p50_ns",
        "max_run_p50_ns",
        "median_p50_ratio_vs_reference",
        "geomean_p50_ratio_vs_reference",
        "median_p99_ns",
        "median_p99_ratio_vs_reference",
        "median_p999_ns",
        "median_p999_ratio_vs_reference",
        "median_validated_round_trips_per_second",
        "median_process_cpu_percent",
        "median_combined_thread_cpu_ns_per_attempt",
        "total_udp_deadline_misses",
        "total_cgroup_throttled_periods",
        "total_cgroup_throttled_usec",
        "validation",
    ]
    with path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.writer(destination, lineterminator="\n")
        writer.writerow(header)
        for payload in payloads:
            for profile in profile_order:
                group = by_group[(profile, payload)]
                reference_by_run = {
                    macro_run: by_macro[
                        (macro_run, reference_profile, payload)
                    ]
                    for macro_run in range(1, len(group) + 1)
                }
                ratios = defaultdict(list)
                wins = 0
                for row in group:
                    macro_run = int(row["macro_run"])
                    reference = reference_by_run[macro_run]
                    for column in ("p50_ns", "p99_ns", "p999_ns"):
                        ratios[column].append(
                            float(row[column]) / float(reference[column])
                        )
                    if float(row["p50_ns"]) < float(reference["p50_ns"]):
                        wins += 1
                combined_cpu = [
                    float(row["client_cpu_ns_per_attempt"])
                    + float(row["server_cpu_ns_per_attempt"])
                    for row in group
                ]
                p50_values = [int(row["p50_ns"]) for row in group]
                output = [
                    profile,
                    payload,
                    len(group),
                    "" if profile == reference_profile else wins,
                    min(p50_values),
                    statistics.median(p50_values),
                    max(p50_values),
                    statistics.median(ratios["p50_ns"]),
                    math.exp(
                        statistics.mean(
                            math.log(value) for value in ratios["p50_ns"]
                        )
                    ),
                    numeric_median(group, "p99_ns"),
                    statistics.median(ratios["p99_ns"]),
                    numeric_median(group, "p999_ns"),
                    statistics.median(ratios["p999_ns"]),
                    numeric_median(group, "validated_round_trips_per_second"),
                    numeric_median(group, "process_cpu_percent"),
                    statistics.median(combined_cpu),
                    sum(int(row["udp_deadline_misses"]) for row in group),
                    sum(int(row["cgroup_throttled_periods"]) for row in group),
                    sum(int(row["cgroup_throttled_usec"]) for row in group),
                    "PASS",
                ]
                writer.writerow(format_number(value) for value in output)


def main():
    arguments = parse_args()
    if not arguments.binary.is_file() or not os.access(arguments.binary, os.X_OK):
        raise ValueError("--binary must name an executable file")
    if arguments.output_dir.exists():
        raise ValueError("output directory already exists")
    allowed_cpus = os.sched_getaffinity(0)
    if (
        arguments.client_cpu not in allowed_cpus
        or arguments.server_cpu not in allowed_cpus
    ):
        raise ValueError("requested CPU is outside this process's affinity mask")
    arguments.output_dir.mkdir(parents=True)

    conditions = [
        (profile, payload)
        for payload in arguments.payloads
        for profile in arguments.profiles
    ]
    first_row = williams_first_row(len(conditions))
    raw_path = arguments.output_dir / "raw.csv"
    summary_path = arguments.output_dir / "summary.csv"
    manifest_path = arguments.output_dir / "manifest.json"
    started = datetime.now(timezone.utc).isoformat()
    raw_rows = []
    invocations = []
    benchmark_fields = None

    for macro_run in range(arguments.observations):
        for position, base_index in enumerate(first_row):
            condition_index = (base_index + macro_run) % len(conditions)
            profile, payload = conditions[condition_index]
            command, stderr, fields, row, diagnostics = run_condition(
                arguments, profile, payload
            )
            if benchmark_fields is None:
                benchmark_fields = fields
            elif fields != benchmark_fields:
                raise ValueError("benchmark CSV schema changed during tournament")
            enriched = {
                "macro_run": str(macro_run + 1),
                "tournament_position": str(position + 1),
                "cgroup_throttled_periods": str(
                    diagnostics["cgroup_throttled_periods"]
                ),
                "cgroup_throttled_usec": str(
                    diagnostics["cgroup_throttled_usec"]
                ),
                **row,
            }
            raw_rows.append(enriched)
            invocations.append({
                "command": command,
                "stderr": stderr,
            })

    validate_tournament(
        raw_rows,
        arguments.profiles,
        arguments.payloads,
        arguments.observations,
    )
    with raw_path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=[*PREFIX_COLUMNS, *benchmark_fields],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(raw_rows)

    write_summary(
        summary_path,
        raw_rows,
        arguments.profiles,
        arguments.payloads,
        arguments.reference_profile,
    )
    manifest = {
        "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "runner": str(Path(__file__).resolve()),
        "runner_sha256": sha256(Path(__file__)),
        "invocation": [sys.executable, *sys.argv],
        "working_directory": str(Path.cwd()),
        "binary": str(arguments.binary.resolve()),
        "binary_sha256": sha256(arguments.binary),
        "protocol": arguments.protocol,
        "payloads": arguments.payloads,
        "profiles": arguments.profiles,
        "reference_profile": arguments.reference_profile,
        "observations": arguments.observations,
        "warmup": arguments.warmup,
        "samples": arguments.samples,
        "client_cpu": arguments.client_cpu,
        "server_cpu": arguments.server_cpu,
        "udp_timeout_ms": arguments.udp_timeout_ms,
        "williams_first_row": first_row,
        "raw_sha256": sha256(raw_path),
        "summary_sha256": sha256(summary_path),
        "invocations": invocations,
    }
    with manifest_path.open("w", encoding="utf-8") as destination:
        json.dump(manifest, destination, indent=2)
        destination.write("\n")

    print(summary_path)


if __name__ == "__main__":
    try:
        main()
    except (
        OSError,
        ValueError,
        RuntimeError,
        subprocess.SubprocessError,
    ) as error:
        print(f"socket profile tournament failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
