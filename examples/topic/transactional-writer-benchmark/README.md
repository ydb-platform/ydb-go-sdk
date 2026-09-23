# Transactional Topic writer benchmark

This command measures the transactional-outbox path used in IOT-10956:

1. execute one `UPSERT` through the Query API;
2. create a transactional Topic writer;
3. write one or more messages;
4. commit the transaction, including the Topic flush.

It covers both transactional writer variants:

- `--mode many`: `WithWriteToManyPartitions`, with key routing by default;
- `--mode single`: a writer without `WithWriteToManyPartitions`; the server chooses a partition from the producer ID.

The defaults reproduce the short baseline protocol: four concurrent workers, a
two-second warmup, a five-second measured phase, one 1024-byte message per
transaction, stable producer IDs per worker, SDK-assigned sequence numbers,
disabled direct write, and standard Query transaction retries. A transaction already
started at the end of a phase is allowed to finish within
`--transaction-timeout`; no new transaction starts after the phase deadline.

## Why this is a command instead of `go test -bench`

`testing.B` is useful for in-process CPU and allocation microbenchmarks. This
workload measures a real database with schema preparation, warmup, fixed
concurrency, transaction latency percentiles, and network-session lifecycle
counters. A standalone command makes those boundaries explicit and emits one
portable JSON report per run.

Prometheus and Grafana are not required for the initial A/B comparison. The JSON
report contains the primary throughput, latency, allocation, and writer
lifecycle metrics. Optional CPU and heap profiles provide local attribution.
Prometheus/Grafana are a useful later addition for long-running or distributed
stands where server and host time series must be correlated.

## Build

Run from the `examples` module:

```bash
go build -o /tmp/transactional-writer-benchmark ./topic/transactional-writer-benchmark
```

## Start local YDB 26.3

The local benchmark results in this PR were collected with
`ydbplatform/local-ydb:26.3.1.16`. Start the container with hostname
`localhost`: endpoint discovery otherwise advertises the generated container
hostname, which is not resolvable from the host.

Use a fresh container for every measured matrix cell. Reusing one server for a
long matrix lets closed StreamWrite sessions accumulate and makes later cells
depend on run order. The helper below removes the previous benchmark container,
starts YDB with in-memory PDisks, and does not return until Docker reports it as
healthy:

```bash
YDB_CONTAINER=tx-writer-benchmark-ydb
YDB_IMAGE=ydbplatform/local-ydb:26.3.1.16

restart_ydb() {
  docker rm -f "$YDB_CONTAINER" >/dev/null 2>&1 || true
  docker run -d \
    --name "$YDB_CONTAINER" \
    --hostname localhost \
    --platform linux/amd64 \
    -p 127.0.0.1:2136:2136 \
    -e GRPC_PORT=2136 \
    -e YDB_USE_IN_MEMORY_PDISKS=true \
    "$YDB_IMAGE" >/dev/null

  for _ in $(seq 1 90); do
    health=$(docker inspect "$YDB_CONTAINER" \
      --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}')
    if [ "$health" = healthy ]; then
      return 0
    fi
    sleep 2
  done

  docker logs "$YDB_CONTAINER"
  return 1
}

stop_ydb() {
  docker rm -f "$YDB_CONTAINER" >/dev/null 2>&1 || true
}
```

The image is currently `linux/amd64`; `--platform linux/amd64` is therefore
needed on Apple Silicon. Remove it when using a native image. Do not prepare or
measure until `restart_ydb` succeeds.

## Prepare a local stand

The command can create the state table and a fixed-partition Topic. Existing
objects are preserved; the actual active partition count is always recorded in
the JSON report.

```bash
/tmp/transactional-writer-benchmark \
  --dsn grpc://localhost:2136/local \
  --topic tx-writer-benchmark-p16 \
  --table tx-writer-benchmark-state \
  --prepare-only \
  --prepare-partitions 16 \
  --anonymous
```

Without `--anonymous`, credentials are loaded through
`ydb-go-sdk-auth-environ`.

## Run both writer modes

The default multiwriter scenario routes by `Message.Key` with
`KafkaHashPartitionChooser`, which works with the fixed-partition Topics used by
the benchmark:

```bash
/tmp/transactional-writer-benchmark \
  --dsn grpc://localhost:2136/local \
  --topic tx-writer-benchmark-p16 \
  --table tx-writer-benchmark-state \
  --mode many \
  --label master \
  --anonymous > master-many-p16.json
```

The single-writer control uses the same transaction and payload:

```bash
/tmp/transactional-writer-benchmark \
  --dsn grpc://localhost:2136/local \
  --topic tx-writer-benchmark-p16 \
  --table tx-writer-benchmark-state \
  --mode single \
  --label master \
  --anonymous > master-single-p16.json
```

Use `--routing partition-id` in `many` mode to reproduce the original loader's
explicit round-robin partition selection. Key routing is the default because it
matches the target `UPSERT + write by key + Commit` scenario. Routing is ignored
in `single` mode.

## Auto-split scenario

Use `--auto-split` to measure the multiwriter while YDB changes the partition
topology under sustained write load. A newly prepared Topic starts with one
active partition, enables `SCALE_UP`, allows up to 64 active partitions, and
uses `BoundPartitionChooser`. Its defaults also change the measured phase to
two minutes, disable the warmup so the initial topology is observable, and
raise the error limit to 100; explicit `--duration`, `--warmup`, and
`--max-errors` values are preserved.

```bash
/tmp/transactional-writer-benchmark \
  --dsn grpc://localhost:2136/local \
  --topic tx-writer-benchmark-autosplit \
  --table tx-writer-benchmark-state \
  --auto-split \
  --prepare \
  --label master \
  --anonymous > master-many-autosplit.json
```

The default Topic policy allows one to 64 active partitions, declares a
1 MiB/s per-partition write speed, and requests scale-up above 2% utilization
with a two-second stabilization window. These deliberately sensitive settings
encourage repeated splits for the small local workload; tune the
`--auto-split-*` flags for a larger stand.

An existing Topic may already have more than one active partition. The
benchmark records that count and continues instead of rejecting the run. It
does not require a split to happen during every measurement and does not delete
or recreate an existing Topic.

The JSON Topic section records initial and final active partition IDs and
counts, total partition objects (including inactive parents), whether a split
was observed, the first observed split time, and the number of topology polls.
The final active partition count is reported even when no split happens during
the run. Polling uses a separate driver, so its `DescribeTopic` calls do not
contaminate the measured writer lifecycle counters.

Query retries are enabled by default, as they are in normal SDK usage. During a
topology transition, an attempt can still receive an error such as
`Partition ... is inactive`; `DoTx` retries the whole idempotent transaction
within `--transaction-timeout`. The report distinguishes logical transactions,
attempts, retries, and final failures. Use `--query-retries=false` only for a
diagnostic run that needs to expose the raw transition errors.

The default producer prefix is unique per process and is extended with
`-slot-N`, giving each sequential worker one stable producer identity without
colliding with earlier runs. Pass `--producer-id-prefix=` to leave the producer
unspecified. Use `--auto-seq-no=false` to exercise manual positive sequence
numbers; the command then preserves the same numbers if a transaction handler
is invoked again.

## Baseline matrix

The fixed-topology matrix contains one control without the multiwriter and one
multiwriter scenario for each supported chooser:

- `single`: writer without `WithWriteToManyPartitions`;
- `many-key`: `KafkaHashPartitionChooser`;
- `many-bounded-key`: `BoundPartitionChooser`;
- `many-partition-id`: explicit partition ID routing.

Run the matrix with 64, 128, 256, and 512 partitions. Kafka hash requires a
Topic without key bounds, while the bounded chooser requires key bounds. The
benchmark therefore prepares two fixed-topology Topics per partition count:
disabled auto-partitioning for `single`, `many-key`, and `many-partition-id`,
and paused auto-partitioning for `many-bounded-key`. Paused auto-partitioning
exposes bounds without changing the fixed partition count.

Alternate `master` and candidate binaries and run each cell at least three
times. Run the following block in the same shell where `restart_ydb` and
`stop_ydb` were defined. It intentionally restarts YDB before every
prepare-and-measure pair. Short defaults are useful for checking the setup; the
example uses a ten-second warmup and a 60-second measurement for more stable
p95/p99 and allocation conclusions.

```bash
BENCHMARK_BIN=/tmp/transactional-writer-benchmark
BENCHMARK_LABEL=master
mkdir -p results

for partitions in 64 128 256 512; do
  for repetition in 1 2 3; do
    for scenario in \
      "single:key:single" \
      "many:key:many-key" \
      "many:bounded-key:many-bounded-key" \
      "many:partition-id:many-partition-id"
    do
      IFS=: read -r mode routing name <<< "$scenario"
      restart_ydb || exit 1

      topic="tx-writer-benchmark-${name}-p${partitions}"
      "$BENCHMARK_BIN" \
        --dsn grpc://localhost:2136/local \
        --topic "$topic" \
        --table tx-writer-benchmark-state \
        --prepare-only \
        --prepare-partitions "$partitions" \
        --routing "$routing" \
        --transaction-timeout 1m \
        --anonymous || exit 1

      "$BENCHMARK_BIN" \
        --dsn grpc://localhost:2136/local \
        --topic "$topic" \
        --table tx-writer-benchmark-state \
        --mode "$mode" \
        --routing "$routing" \
        --label "$BENCHMARK_LABEL" \
        --warmup 10s \
        --duration 60s \
        --transaction-timeout 1m \
        --max-errors 100 \
        --anonymous \
        > "results/${BENCHMARK_LABEL}-${name}-p${partitions}-${repetition}.json" || exit 1

      test "$(docker inspect "$YDB_CONTAINER" \
        --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}')" \
        = healthy || exit 1
    done
  done
done

stop_ydb
```

Repeat the same matrix with the candidate binary and a different
`BENCHMARK_LABEL`. Run the bounded auto-split scenario separately at least three
times. The example below restarts YDB for every repetition so every run observes
a fresh `1 -> N` transition. Keep the YDB version, resources, Topic settings,
run order, and all CLI flags identical between compared revisions.

```bash
for repetition in 1 2 3; do
  restart_ydb || exit 1

  "$BENCHMARK_BIN" \
    --dsn grpc://localhost:2136/local \
    --topic tx-writer-benchmark-autosplit \
    --table tx-writer-benchmark-state \
    --auto-split \
    --prepare \
    --label "$BENCHMARK_LABEL" \
    --transaction-timeout 1m \
    --max-errors 100 \
    --anonymous \
    > "results/${BENCHMARK_LABEL}-many-bounded-autosplit-${repetition}.json" || exit 1

  test "$(docker inspect "$YDB_CONTAINER" \
    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}')" \
    = healthy || exit 1
done

stop_ydb
```

## Metrics and profiles

Each JSON report includes:

- committed transactions/messages, TPS, messages/s, and payload MiB/s;
- p50/p95/p99 for the full transaction, table call, writer construction, and `Write`;
- final errors, cancellations, attempts, and retries;
- measured-phase `DescribeTopic`, `StreamWrite`, writer init/close, WriteRequest,
  batch-message, ACK, `written_in_tx`, and `skipped` counters;
- Go allocation count/bytes, measured GC cycles, heap at phase stop, and retained heap after a forced post-phase GC;
- SDK/Go versions, VCS revision when available, CPU architecture, and all workload settings.

Capture profiles only on dedicated profiling runs because CPU profiling adds a
small amount of overhead:

```bash
/tmp/transactional-writer-benchmark \
  --dsn grpc://localhost:2136/local \
  --topic tx-writer-benchmark-p64 \
  --table tx-writer-benchmark-state \
  --mode many \
  --duration 60s \
  --warmup 10s \
  --cpu-profile master-many-p64.cpu.pprof \
  --heap-profile master-many-p64.heap.pprof \
  --anonymous > master-many-p64-profile.json

go tool pprof -http=:0 /tmp/transactional-writer-benchmark master-many-p64.cpu.pprof
```

For a valid A/B run, require at least one committed transaction,
`failed_transactions=0`, and `aborted_by_error_limit=false`. Retries are part of
the workload and must be compared alongside throughput and latency. Compare
medians and spread across repetitions, not only the best result.
