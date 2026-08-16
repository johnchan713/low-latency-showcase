#!/usr/bin/env bash

set -euo pipefail

print_section() {
    local title=$1
    printf '\n## %s\n' "$title"
}

run_if_available() {
    local executable=$1
    local command_status
    shift

    if command -v "$executable" >/dev/null 2>&1; then
        if "$executable" "$@"; then
            return 0
        else
            command_status=$?
            printf '%s exited with status %s; collection continues\n' \
                "$executable" "$command_status" >&2
        fi
    else
        printf '%s is not available\n' "$executable"
    fi
}

print_section "Timestamp"
date --iso-8601=seconds

print_section "Kernel"
uname -a

print_section "CPU"
run_if_available lscpu

print_section "NUMA"
run_if_available numactl --hardware

print_section "GCC"
run_if_available g++ --version

print_section "Clang"
run_if_available clang++ --version

print_section "CMake"
run_if_available cmake --version

print_section "Ninja"
run_if_available ninja --version

print_section "Power governor"
governor_files=(/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor)
if [[ -e "${governor_files[0]}" ]]; then
    if ! sort -u "${governor_files[@]}"; then
        printf 'Could not read all CPU governor information\n' >&2
    fi
else
    printf 'CPU governor information is not exposed\n'
fi

printf '\nReview this output before publishing it.\n'
