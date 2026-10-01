# Transactional Topic writer benchmark

This package contains standard Go benchmarks for a transactional-outbox path:

1. create a transactional Topic writer while the Query transaction is lazy;
2. execute one `UPSERT` through the Query API, materializing the transaction;
3. write one 1024-byte message through the already-created writer;
4. commit the transaction, including the Topic flush.

Every attempt uses `query.WithLazyTx(true)`. Creating the writer before the
`UPSERT` exercises the transition from the lazy transaction ID to the materialized
transaction before the writer calls `UnLazy` during `Write`.

`BenchmarkTransactionalWriter` covers four fixed-topology scenarios for every
requested partition count:

- `single`: writer without `WithWriteToManyPartitions`;
- `many-key`: `WithWriteToManyPartitions` and `KafkaHashPartitionChooser`;
- `many-bounded-key`: `WithWriteToManyPartitions` and `BoundPartitionChooser`;
- `many-partition-id`: `WithWriteToManyPartitions` and explicit partition IDs.

`BenchmarkTransactionalWriterAutoSplit` exercises the bounded-key writer while
YDB splits a Topic from one active partition. The benchmark sets
`max_active_partitions` to YDB's server-wide ceiling (35,000), because omitting
it makes YDB use `min_active_partitions` as the maximum and disables splitting.
The scenario therefore does not impose a lower partition limit of its own.

## Start local YDB

The baseline recorded in the source was measured with
`ydbplatform/local-ydb:26.3.1.16`. A fresh database is recommended for a
comparison so previously created Topics cannot affect the topology.

```bash
docker run --rm -d \
  --name tx-writer-benchmark-ydb \
  --hostname localhost \
  --platform linux/amd64 \
  -p 127.0.0.1:2136:2136 \
  -e GRPC_PORT=2136 \
  -e YDB_USE_IN_MEMORY_PDISKS=true \
  ydbplatform/local-ydb:26.3.1.16
```

Wait until Docker reports the container as healthy before starting a benchmark.
The image is currently `linux/amd64`; `--platform linux/amd64` is needed on
Apple Silicon.

## Fixed-topology matrix

Run the benchmark from the repository root. The benchmark prepares the state
table and all required Topics before starting the timer. `b.N` is the number of
logical transactions and `RunParallel` distributes them over the workers
selected by `-cpu`.

```bash
go test ./tests/integration/transactional-writer-benchmark \
  -run '^$' \
  -bench '^BenchmarkTransactionalWriter$' \
  -benchtime=10s \
  -count=3 \
  -cpu=4 \
  -args \
  -ydb-benchmark-dsn grpc://localhost:2136/local \
  -ydb-benchmark-partitions 64,128,256,512
```

The standard `ns/op`, `B/op`, and `allocs/op` columns are accompanied by:

- `tx/s`: aggregate committed transaction throughput;
- `ms/p50`, `ms/p95`, and `ms/p99`: full transaction latency;
- `ms/table-p95`, `ms/writer-start-p95`, and `ms/writer-write-p95`:
  component latency;
- `errors` and `errors/tx`: final transaction errors, as a count and a ratio;
- `retries/tx`: Query transaction retries per logical transaction;
- `StreamWrite/tx`: Topic StreamWrite sessions opened per commit;
- `workers`: effective `GOMAXPROCS` and parallel worker count;
- `measured-s`: duration of the timed region.

Final transaction errors are recorded but do not stop or fail the measurement.
Setup and connection errors still fail the benchmark. Query retries remain
enabled because that matches normal SDK usage.

The default DSN can also be supplied through `YDB_CONNECTION_STRING`. Use
`-ydb-benchmark-topic-prefix` and `-ydb-benchmark-table` when the default object
names would collide with another run.

## Auto-split scenario

Auto-split has a time-dependent topology, so it deliberately defines one
benchmark operation as one two-minute phase. Run it with `-benchtime=1x`; using
the usual adaptive `b.N` calibration would measure successive, different
topologies. Run each repetition against a fresh YDB container so accumulated
StreamWrite sessions and prior splits do not affect the next result.

When the two-minute phase ends, workers do not start another transaction. A
transaction already in progress may finish, but its total execution time is
still limited to one minute.

```bash
go test ./tests/integration/transactional-writer-benchmark \
  -run '^$' \
  -bench '^BenchmarkTransactionalWriterAutoSplit$' \
  -benchtime=1x \
  -count=1 \
  -cpu=4 \
  -args \
  -ydb-benchmark-dsn grpc://localhost:2136/local
```

Repeat this command three times, restarting the YDB container before each run.

This benchmark describes the Topic once before the measured phase and once
after it, then reports the final `active-partitions`. Because `b.N` is one, its
standard `B/op` and `allocs/op` columns describe the complete two-minute phase;
use `tx/s` and latency metrics for throughput and response-time comparisons.

## Compare revisions

Capture raw Go benchmark output for the base and candidate revisions while
keeping the Go version, YDB image, CPU count, partition list, and command-line
flags identical:

```bash
go test ./tests/integration/transactional-writer-benchmark \
  -run '^$' -bench '^BenchmarkTransactionalWriter$' \
  -benchtime=10s -count=3 -cpu=4 \
  -args -ydb-benchmark-dsn grpc://localhost:2136/local \
  > master.txt

# Check out the candidate revision, start a fresh YDB container, and repeat:
go test ./tests/integration/transactional-writer-benchmark \
  -run '^$' -bench '^BenchmarkTransactionalWriter$' \
  -benchtime=10s -count=3 -cpu=4 \
  -args -ydb-benchmark-dsn grpc://localhost:2136/local \
  > candidate.txt

benchstat master.txt candidate.txt
```

Compare medians and spread across repetitions, not only the best result. The
master baseline and its exact environment are recorded in a comment near the
benchmark implementation so a later PR can compare against the same protocol.
