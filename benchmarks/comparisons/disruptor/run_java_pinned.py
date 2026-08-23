#!/usr/bin/env python3
"""Launch Java, pin its two named workers, then open their start gate."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time


PRODUCER_NAME = "lls-producer"
CONSUMER_NAME = "lls-consumer"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--producer-cpu", type=int, required=True)
    parser.add_argument("--consumer-cpu", type=int, required=True)
    parser.add_argument("--gate", type=Path, required=True)
    parser.add_argument("--stdout", type=Path, required=True)
    parser.add_argument(
        "--main-class",
        default="PairedBenchmark",
        help="exact Java main-class token used to resolve the host /proc PID",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    result = parser.parse_args()
    if result.command and result.command[0] == "--":
        result.command = result.command[1:]
    if not result.command:
        parser.error("a Java command is required after --")
    return result


def read_thread_names(pid: int) -> dict[int, str]:
    tasks = Path(f"/proc/{pid}/task")
    output: dict[int, str] = {}
    try:
        children = list(tasks.iterdir())
    except FileNotFoundError:
        return output
    for child in children:
        try:
            output[int(child.name)] = (child / "comm").read_text().strip()
        except (FileNotFoundError, ProcessLookupError):
            continue
    return output


def find_java_pids(gate_token: str, main_class: str) -> list[int]:
    # Some sandbox/container launchers expose host PIDs in /proc while Popen
    # reports the child namespace PID. The unique gate path is also present in
    # the Java command line, so resolve the auditable /proc PID explicitly.
    matches: list[int] = []
    for candidate in Path("/proc").iterdir():
        if not candidate.name.isdigit():
            continue
        try:
            command = (candidate / "cmdline").read_bytes().split(b"\0")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        decoded = [part.decode(errors="replace") for part in command if part]
        if not decoded:
            continue
        executable = Path(decoded[0]).name
        if executable.startswith("java") and main_class in decoded \
                and gate_token in decoded:
            matches.append(int(candidate.name))
    return matches


def namespace_tid(proc_pid: int, host_tid: int) -> int | None:
    try:
        status = Path(
            f"/proc/{proc_pid}/task/{host_tid}/status"
        ).read_text().splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    for line in status:
        if line.startswith("NSpid:"):
            identifiers = line.split()[1:]
            return int(identifiers[-1]) if identifiers else None
    return host_tid


def pin_if_alive(tid: int, cpus: set[int]) -> bool:
    try:
        os.sched_setaffinity(tid, cpus)
        return True
    except ProcessLookupError:
        return False


def affinity_if_alive(tid: int) -> set[int] | None:
    try:
        return os.sched_getaffinity(tid)
    except ProcessLookupError:
        return None


def parse_cpu_list(text: str) -> set[int]:
    output: set[int] = set()
    for part in text.strip().split(","):
        limits = part.split("-", maxsplit=1)
        first = int(limits[0])
        last = int(limits[-1])
        output.update(range(first, last + 1))
    return output


def thread_siblings(cpu: int) -> set[int]:
    path = Path(
        f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
    )
    return parse_cpu_list(path.read_text())


def terminate_and_reap(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        process.wait()
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def main() -> int:
    arguments = parse_arguments()
    allowed = os.sched_getaffinity(0)
    requested = {arguments.producer_cpu, arguments.consumer_cpu}
    if len(requested) != 2 or not requested.issubset(allowed):
        raise SystemExit(
            f"requested worker CPUs {sorted(requested)} are not two distinct "
            f"members of allowed set {sorted(allowed)}"
        )
    producer_siblings = thread_siblings(arguments.producer_cpu)
    consumer_siblings = thread_siblings(arguments.consumer_cpu)
    if arguments.consumer_cpu in producer_siblings:
        raise SystemExit("producer and consumer CPUs must not be SMT siblings")
    housekeeping = allowed - producer_siblings - consumer_siblings
    if not housekeeping:
        raise SystemExit(
            "no housekeeping CPU remains after excluding both hot cores and "
            "all of their SMT siblings"
        )
    # Keep the affinity monitor itself off the benchmark pair.
    os.sched_setaffinity(0, housekeeping)

    arguments.gate.parent.mkdir(parents=True, exist_ok=True)
    arguments.stdout.parent.mkdir(parents=True, exist_ok=True)
    arguments.gate.unlink(missing_ok=True)

    with arguments.stdout.open("w", encoding="utf-8", newline="") as output:
        process = subprocess.Popen(
            arguments.command,
            stdout=output,
            stderr=None,
            text=True,
        )

        try:
            deadline = time.monotonic() + 30.0
            proc_pid: int | None = None
            producer_tid: int | None = None
            consumer_tid: int | None = None
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    return process.wait()

                # Never interpret Popen's namespace-local PID as a /proc PID.
                # Resolve the exact host-visible Java process from this
                # sample's unique gate argument before reading any task names.
                java_pids = find_java_pids(
                    str(arguments.gate), arguments.main_class
                )
                if len(java_pids) > 1:
                    raise RuntimeError(
                        "multiple Java processes match the unique affinity gate"
                    )
                if not java_pids:
                    time.sleep(0.002)
                    continue
                proc_pid = java_pids[0]
                names = read_thread_names(proc_pid)
                producer_matches = [
                    host_tid
                    for host_tid, name in names.items()
                    if name == PRODUCER_NAME
                ]
                consumer_matches = [
                    host_tid
                    for host_tid, name in names.items()
                    if name == CONSUMER_NAME
                ]
                # pthread creation transiently inherits the parent's OS comm.
                # A newborn consumer can therefore appear as a second
                # lls-producer until HotSpot applies its Java thread name.
                # Treat anything except one settled worker of each name as a
                # discovery-in-progress state, bounded by the outer deadline.
                if len(producer_matches) != 1 or len(consumer_matches) != 1:
                    time.sleep(0.002)
                    continue

                producer_tid = namespace_tid(proc_pid, producer_matches[0])
                consumer_tid = namespace_tid(proc_pid, consumer_matches[0])
                if producer_tid is None or consumer_tid is None:
                    time.sleep(0.002)
                    continue
                for host_tid in names:
                    tid = namespace_tid(proc_pid, host_tid)
                    if tid is None:
                        continue
                    if tid == producer_tid:
                        pin_if_alive(tid, {arguments.producer_cpu})
                    elif tid == consumer_tid:
                        pin_if_alive(tid, {arguments.consumer_cpu})
                    else:
                        pin_if_alive(tid, housekeeping)

                if affinity_if_alive(producer_tid) != {
                    arguments.producer_cpu
                }:
                    raise RuntimeError("producer affinity verification failed")
                if affinity_if_alive(consumer_tid) != {
                    arguments.consumer_cpu
                }:
                    raise RuntimeError("consumer affinity verification failed")

                # Prove that the same two selected TIDs remain uniquely named
                # after a settle window. Extra inherited names are transient,
                # so retry them within the original bounded deadline.
                settled_names: dict[int, str] = {}
                while time.monotonic() < deadline:
                    time.sleep(0.05)
                    settled_pids = find_java_pids(
                        str(arguments.gate), arguments.main_class
                    )
                    if len(settled_pids) > 1:
                        raise RuntimeError(
                            "multiple Java processes match the unique gate "
                            "during affinity settle"
                        )
                    if settled_pids != [proc_pid]:
                        if process.poll() is not None:
                            return process.wait()
                        continue
                    settled_names = read_thread_names(proc_pid)
                    settled_producers = [
                        host_tid
                        for host_tid, name in settled_names.items()
                        if name == PRODUCER_NAME
                    ]
                    settled_consumers = [
                        host_tid
                        for host_tid, name in settled_names.items()
                        if name == CONSUMER_NAME
                    ]

                    # Affinity work is identity-based after selection. Any
                    # other TID is housekeeping even if its comm is transiently
                    # inherited from a hot worker.
                    for host_tid in settled_names:
                        tid = namespace_tid(proc_pid, host_tid)
                        if tid is None:
                            continue
                        if tid == producer_tid:
                            pin_if_alive(tid, {arguments.producer_cpu})
                        elif tid == consumer_tid:
                            pin_if_alive(tid, {arguments.consumer_cpu})
                        else:
                            pin_if_alive(tid, housekeeping)

                    if len(settled_producers) != 1 \
                            or len(settled_consumers) != 1:
                        continue
                    settled_producer_tid = namespace_tid(
                        proc_pid, settled_producers[0]
                    )
                    settled_consumer_tid = namespace_tid(
                        proc_pid, settled_consumers[0]
                    )
                    if settled_producer_tid == producer_tid \
                            and settled_consumer_tid == consumer_tid:
                        break
                else:
                    raise RuntimeError(
                        "named Java worker TIDs did not settle before timeout"
                    )

                if affinity_if_alive(producer_tid) != {
                    arguments.producer_cpu
                } or affinity_if_alive(consumer_tid) != {
                    arguments.consumer_cpu
                }:
                    raise RuntimeError(
                        "worker affinity changed during settle verification"
                    )
                arguments.gate.touch(exist_ok=False)

                # Keep late-created JVM threads off the hot pair and audit
                # both worker masks without resetting them during timing.
                while process.poll() is None:
                    for host_tid in read_thread_names(proc_pid):
                        tid = namespace_tid(proc_pid, host_tid)
                        if tid is None:
                            continue
                        if tid == producer_tid:
                            current = affinity_if_alive(tid)
                            if current is not None and current != {
                                arguments.producer_cpu
                            }:
                                raise RuntimeError(
                                    "producer affinity changed during run"
                                )
                        elif tid == consumer_tid:
                            current = affinity_if_alive(tid)
                            if current is not None and current != {
                                arguments.consumer_cpu
                            }:
                                raise RuntimeError(
                                    "consumer affinity changed during run"
                                )
                        else:
                            current = affinity_if_alive(tid)
                            if current != housekeeping:
                                pin_if_alive(tid, housekeeping)
                    time.sleep(0.05)
                return process.wait()

            raise RuntimeError("timed out discovering named Java worker threads")
        except BaseException:
            terminate_and_reap(process)
            raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Java affinity launcher: {error}", file=sys.stderr)
        raise SystemExit(1)
