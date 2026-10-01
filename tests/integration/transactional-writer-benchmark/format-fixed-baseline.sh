#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <go-benchmark-output>" >&2
  exit 2
fi

input=$1
if [[ ! -r "$input" ]]; then
  echo "cannot read benchmark output: $input" >&2
  exit 2
fi

LC_ALL=C awk '
function median3(a, b, c, low, high) {
  low = a < b ? (a < c ? a : c) : (b < c ? b : c)
  high = a > b ? (a > c ? a : c) : (b > c ? b : c)
  return a + b + c - low - high
}

function emit(parts, partition) {
  if (samples == 0) {
    return
  }
  if (samples != 3) {
    printf "expected 3 samples for %s, got %d\n", current, samples > "/dev/stderr"
    status = 1
    samples = 0
    return
  }

  split(current, parts, "/")
  partition = parts[3]
  sub(/^p/, "", partition)
  printf "//\t%-4s %-22s %8.4g %9.4g %9.0f %10.0f %15.4g\n", \
    partition,
    parts[2],
    median3(tx[1], tx[2], tx[3]),
    median3(p95[1], p95[2], p95[3]),
    median3(bytes[1], bytes[2], bytes[3]),
    median3(allocs[1], allocs[2], allocs[3]),
    median3(streams[1], streams[2], streams[3])
  rows++
  samples = 0
}

BEGIN {
  print "//\tP    scenario                   tx/s    p95 ms      B/op  allocs/op  StreamWrite/tx"
}

$1 ~ /^BenchmarkTransactionalWriter\// {
  name = $1
  sub(/-[0-9]+$/, "", name)
  if (current != "" && name != current) {
    emit()
  }
  current = name
  samples++
  foundTx = foundP95 = foundBytes = foundAllocs = foundStreams = 0

  for (i = 2; i <= NF; i++) {
    if ($i == "tx/s") {
      tx[samples] = $(i - 1)
      foundTx = 1
    } else if ($i == "ms/p95") {
      p95[samples] = $(i - 1)
      foundP95 = 1
    } else if ($i == "B/op") {
      bytes[samples] = $(i - 1)
      foundBytes = 1
    } else if ($i == "allocs/op") {
      allocs[samples] = $(i - 1)
      foundAllocs = 1
    } else if ($i == "StreamWrite/tx") {
      streams[samples] = $(i - 1)
      foundStreams = 1
    }
  }
  if (!(foundTx && foundP95 && foundBytes && foundAllocs && foundStreams)) {
    printf "missing required metrics in sample for %s\n", current > "/dev/stderr"
    status = 1
  }
}

END {
  emit()
  if (rows == 0) {
    print "no BenchmarkTransactionalWriter samples found" > "/dev/stderr"
    status = 1
  }
  exit status
}
' "$input"
