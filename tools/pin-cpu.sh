#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 ]]; then
    printf 'Usage: %s <cpu-list> <command> [arguments...]\n' "$0" >&2
    printf 'Example: %s 2-3 ./build/benchmark-native/example_benchmark\n' "$0" >&2
    exit 64
fi

cpu_list=$1
shift

if ! command -v taskset >/dev/null 2>&1; then
    printf 'taskset is required to pin a process on Linux\n' >&2
    exit 69
fi

exec taskset --cpu-list "$cpu_list" "$@"
