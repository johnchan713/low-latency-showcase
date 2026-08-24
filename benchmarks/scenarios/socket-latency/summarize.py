#!/usr/bin/env python3

import argparse
import csv
import math
import statistics
import sys
from collections import Counter, defaultdict


EXPECTED_TREATMENTS = tuple(
    (protocol, payload)
    for payload in (8, 64, 256, 1400)
    for protocol in ("tcp", "udp")
)

INTEGER_COLUMNS = {
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
    "client_voluntary_context_switches",
    "client_involuntary_context_switches",
    "server_voluntary_context_switches",
    "server_involuntary_context_switches",
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
}

FLOAT_COLUMNS = {
    "validated_round_trips_per_second",
    "application_messages_per_second",
    "client_cpu_percent",
    "server_cpu_percent",
    "process_cpu_percent",
    "client_cpu_ns_per_attempt",
    "server_cpu_ns_per_attempt",
}

PERCENTILE_COLUMNS = (
    "min_ns",
    "p50_ns",
    "p90_ns",
    "p95_ns",
    "p99_ns",
    "p999_ns",
    "max_ns",
)

UDP_EVENT_COLUMNS = (
    "udp_combined_duplicate_events",
    "udp_combined_reordered_events",
    "udp_combined_invalid_events",
    "udp_combined_truncated_events",
)


def positive_integer(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate and summarize balanced socket benchmark rows"
    )
    parser.add_argument("csv_file", help="raw benchmark CSV")
    parser.add_argument(
        "--expected-runs",
        required=True,
        type=positive_integer,
        help="declared run count; audited summaries require a multiple of eight",
    )
    return parser.parse_args()


def median(rows, column):
    return statistics.median(row[column] for row in rows)


def format_number(value):
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.3f}"


def load_rows(path):
    with open(path, newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        required = INTEGER_COLUMNS | FLOAT_COLUMNS | {"protocol", "validation"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"missing CSV columns: {sorted(missing)}")
        rows = []
        for line_number, raw in enumerate(reader, start=2):
            if raw["validation"] != "PASS":
                raise ValueError(f"row {line_number} failed benchmark validation")
            try:
                row = dict(raw)
                for column in INTEGER_COLUMNS:
                    row[column] = int(row[column])
                for column in FLOAT_COLUMNS:
                    row[column] = float(row[column])
            except ValueError as error:
                raise ValueError(
                    f"row {line_number} contains a malformed number"
                ) from error
            validate_row(row, line_number)
            rows.append(row)
    if not rows:
        raise ValueError("raw CSV contains no result rows")
    return rows


def validate_row(row, line_number):
    successful = row["successful_samples"]
    attempted = row["attempted_samples"]
    if successful <= 0 or attempted <= 0 or successful > attempted:
        raise ValueError(f"row {line_number} has invalid sample accounting")
    if row["warmup"] <= 0 or row["udp_deadline_ms"] <= 0:
        raise ValueError(f"row {line_number} has an invalid warm-up or deadline")
    percentiles = [row[column] for column in PERCENTILE_COLUMNS]
    if percentiles[0] <= 0 or percentiles != sorted(percentiles):
        raise ValueError(f"row {line_number} has invalid latency percentiles")
    if any(not math.isfinite(row[column]) for column in FLOAT_COLUMNS):
        raise ValueError(f"row {line_number} has a non-finite metric")
    positive_metrics = (
        "validated_round_trips_per_second",
        "application_messages_per_second",
        "client_cpu_percent",
        "server_cpu_percent",
        "process_cpu_percent",
        "client_cpu_ns_per_attempt",
        "server_cpu_ns_per_attempt",
    )
    if any(row[column] <= 0 for column in positive_metrics):
        raise ValueError(f"row {line_number} has a non-positive rate or CPU metric")
    if abs(
        row["application_messages_per_second"]
        - 2.0 * row["validated_round_trips_per_second"]
    ) > 0.002:
        raise ValueError(f"row {line_number} has inconsistent message throughput")
    buffer_columns = (
        "client_send_buffer_bytes",
        "client_receive_buffer_bytes",
        "server_send_buffer_bytes",
        "server_receive_buffer_bytes",
    )
    if any(row[column] <= 0 for column in buffer_columns):
        raise ValueError(f"row {line_number} has an invalid socket buffer default")
    if row["checksum"] <= 0:
        raise ValueError(f"row {line_number} has an invalid checksum")
    nonnegative_columns = (
        "client_voluntary_context_switches",
        "client_involuntary_context_switches",
        "server_voluntary_context_switches",
        "server_involuntary_context_switches",
        "udp_deadline_misses",
        "udp_requests_unobserved",
        "udp_response_deadline_misses",
        *UDP_EVENT_COLUMNS,
    )
    if any(row[column] < 0 for column in nonnegative_columns):
        raise ValueError(f"row {line_number} has a negative event count")

    if row["protocol"] == "tcp":
        if successful != attempted or row["udp_deadline_misses"] != 0:
            raise ValueError(f"row {line_number} has incomplete TCP accounting")
        if row["client_tcp_nodelay"] != 0 or row["server_tcp_nodelay"] != 0:
            raise ValueError(f"row {line_number} is not a default-Nagle TCP run")
        if row["tcp_connect_ns"] <= 0:
            raise ValueError(f"row {line_number} lacks a TCP connect diagnostic")
        tcp_zero_columns = (
            "udp_requests_unobserved",
            "udp_response_deadline_misses",
            *UDP_EVENT_COLUMNS,
        )
        if any(row[column] != 0 for column in tcp_zero_columns):
            raise ValueError(f"row {line_number} has UDP events in a TCP run")
    elif row["protocol"] == "udp":
        deadline_misses = attempted - successful
        if row["udp_deadline_misses"] != deadline_misses:
            raise ValueError(f"row {line_number} has inconsistent deadline misses")
        if (
            row["udp_requests_unobserved"]
            + row["udp_response_deadline_misses"]
            != deadline_misses
        ):
            raise ValueError(f"row {line_number} has inconsistent UDP accounting")
        if row["client_tcp_nodelay"] != -1 or row["server_tcp_nodelay"] != -1:
            raise ValueError(f"row {line_number} has invalid UDP TCP_NODELAY fields")
        if row["tcp_connect_ns"] != 0:
            raise ValueError(f"row {line_number} has a UDP connect diagnostic")
    else:
        raise ValueError(f"row {line_number} has an unknown protocol")


def validate_design(rows, expected_runs):
    width = len(EXPECTED_TREATMENTS)
    if expected_runs % width != 0:
        raise ValueError("expected runs must be a multiple of eight")
    if len(rows) != expected_runs * width:
        raise ValueError(
            f"expected {expected_runs * width} rows, found {len(rows)}"
        )
    run_ids = {row["run"] for row in rows}
    if run_ids != set(range(1, expected_runs + 1)):
        raise ValueError("run IDs are not contiguous from 1 through expected runs")
    configurations = {
        (
            row["attempted_samples"],
            row["warmup"],
            row["udp_deadline_ms"],
        )
        for row in rows
    }
    if len(configurations) != 1:
        raise ValueError("rows mix sample, warm-up, or deadline configurations")

    expected_treatments = set(EXPECTED_TREATMENTS)
    expected_positions = set(range(1, width + 1))
    by_run = defaultdict(list)
    for row in rows:
        by_run[row["run"]].append(row)

    for run in range(1, expected_runs + 1):
        run_rows = by_run[run]
        positions = {row["workload_position"] for row in run_rows}
        treatments = {
            (row["protocol"], row["payload_bytes"]) for row in run_rows
        }
        if (
            len(run_rows) != width
            or positions != expected_positions
            or treatments != expected_treatments
        ):
            raise ValueError(f"run {run} is not one complete treatment row")

    expected_predecessors = {
        (left, right)
        for left in expected_treatments
        for right in expected_treatments
        if left != right
    }
    for block_start in range(1, expected_runs + 1, width):
        block_runs = range(block_start, block_start + width)
        for treatment in expected_treatments:
            positions = {
                row["workload_position"]
                for run in block_runs
                for row in by_run[run]
                if (row["protocol"], row["payload_bytes"]) == treatment
            }
            if positions != expected_positions:
                raise ValueError(
                    f"runs {block_start}-{block_start + width - 1} "
                    "do not balance treatment positions"
                )

        predecessors = []
        for run in block_runs:
            ordered = sorted(by_run[run], key=lambda row: row["workload_position"])
            treatments = [
                (row["protocol"], row["payload_bytes"]) for row in ordered
            ]
            predecessors.extend(zip(treatments, treatments[1:]))
        if Counter(predecessors) != Counter(
            {pair: 1 for pair in expected_predecessors}
        ):
            raise ValueError(
                f"runs {block_start}-{block_start + width - 1} "
                "do not balance directed predecessor pairs"
            )


def summarize(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["protocol"], row["payload_bytes"])].append(row)

    header = [
        "protocol",
        "payload_bytes",
        "runs",
        "udp_deadline_ms",
        "total_successful_samples",
        "total_attempted_samples",
        "median_min_ns",
        "min_run_p50_ns",
        "median_p50_ns",
        "max_run_p50_ns",
        "median_p90_ns",
        "median_p95_ns",
        "min_run_p99_ns",
        "median_p99_ns",
        "max_run_p99_ns",
        "min_run_p999_ns",
        "median_p999_ns",
        "max_run_p999_ns",
        "median_run_max_ns",
        "global_max_ns",
        "min_run_validated_round_trips_per_second",
        "median_validated_round_trips_per_second",
        "max_run_validated_round_trips_per_second",
        "median_application_messages_per_second",
        "median_client_cpu_percent",
        "median_server_cpu_percent",
        "min_run_process_cpu_percent",
        "median_process_cpu_percent",
        "max_run_process_cpu_percent",
        "median_client_cpu_ns_per_attempt",
        "median_server_cpu_ns_per_attempt",
        "total_udp_deadline_misses",
        "total_udp_requests_unobserved",
        "total_udp_response_deadline_misses",
        "total_udp_combined_duplicate_events",
        "total_udp_combined_reordered_events",
        "total_udp_combined_invalid_events",
        "total_udp_combined_truncated_events",
        "validation",
    ]
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(header)
    protocol_order = {"tcp": 0, "udp": 1}
    for key in sorted(groups, key=lambda item: (protocol_order[item[0]], item[1])):
        group = groups[key]
        if sum(row["udp_deadline_misses"] for row in group) == 0:
            if len({row["checksum"] for row in group}) != 1:
                raise ValueError(f"unstable checksum for {key}")
        output = [
            key[0],
            key[1],
            len(group),
            group[0]["udp_deadline_ms"],
            sum(row["successful_samples"] for row in group),
            sum(row["attempted_samples"] for row in group),
            median(group, "min_ns"),
            min(row["p50_ns"] for row in group),
            median(group, "p50_ns"),
            max(row["p50_ns"] for row in group),
            median(group, "p90_ns"),
            median(group, "p95_ns"),
            min(row["p99_ns"] for row in group),
            median(group, "p99_ns"),
            max(row["p99_ns"] for row in group),
            min(row["p999_ns"] for row in group),
            median(group, "p999_ns"),
            max(row["p999_ns"] for row in group),
            median(group, "max_ns"),
            max(row["max_ns"] for row in group),
            min(row["validated_round_trips_per_second"] for row in group),
            median(group, "validated_round_trips_per_second"),
            max(row["validated_round_trips_per_second"] for row in group),
            median(group, "application_messages_per_second"),
            median(group, "client_cpu_percent"),
            median(group, "server_cpu_percent"),
            min(row["process_cpu_percent"] for row in group),
            median(group, "process_cpu_percent"),
            max(row["process_cpu_percent"] for row in group),
            median(group, "client_cpu_ns_per_attempt"),
            median(group, "server_cpu_ns_per_attempt"),
            sum(row["udp_deadline_misses"] for row in group),
            sum(row["udp_requests_unobserved"] for row in group),
            sum(row["udp_response_deadline_misses"] for row in group),
            sum(row["udp_combined_duplicate_events"] for row in group),
            sum(row["udp_combined_reordered_events"] for row in group),
            sum(row["udp_combined_invalid_events"] for row in group),
            sum(row["udp_combined_truncated_events"] for row in group),
            "PASS",
        ]
        writer.writerow(format_number(value) for value in output)


def main():
    arguments = parse_args()
    rows = load_rows(arguments.csv_file)
    validate_design(rows, arguments.expected_runs)
    summarize(rows)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        print(f"socket benchmark summary failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
