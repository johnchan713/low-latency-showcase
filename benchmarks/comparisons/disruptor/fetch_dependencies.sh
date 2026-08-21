#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
lock_file="$script_dir/dependencies.lock"
destination=${1:-"$script_dir/.deps"}

read_lock_value() {
    local key=$1
    awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' \
        "$lock_file"
}

version=$(read_lock_value disruptor.version)
url=$(read_lock_value disruptor.url)
expected_sha=$(read_lock_value disruptor.sha256)
expected_bytes=$(read_lock_value disruptor.bytes)

if [[ -z "$version" || -z "$url" || -z "$expected_sha" || \
      -z "$expected_bytes" ]]; then
    echo "incomplete dependency lock: $lock_file" >&2
    exit 1
fi

mkdir -p -- "$destination"
jar="$destination/disruptor-$version.jar"

verify_jar() {
    [[ -f "$jar" ]] || return 1
    [[ $(wc -c < "$jar") == "$expected_bytes" ]] || return 1
    [[ $(sha256sum "$jar" | awk '{print $1}') == "$expected_sha" ]]
}

if ! verify_jar; then
    temporary=$(mktemp "$destination/.disruptor-download.XXXXXX")
    cleanup() {
        rm -f -- "$temporary"
    }
    trap cleanup EXIT
    curl --fail --location --silent --show-error "$url" --output "$temporary"
    [[ $(wc -c < "$temporary") == "$expected_bytes" ]] || {
        echo "downloaded Disruptor jar has the wrong size" >&2
        exit 1
    }
    [[ $(sha256sum "$temporary" | awk '{print $1}') == "$expected_sha" ]] || {
        echo "downloaded Disruptor jar failed SHA-256 verification" >&2
        exit 1
    }
    mv -- "$temporary" "$jar"
    trap - EXIT
fi

printf '%s\n' "$jar"
