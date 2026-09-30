# Transactional Topic writer benchmark

This package contains standard Go benchmarks for a transactional-outbox path:

1. execute one `UPSERT` through the Query API;
2. create a transactional Topic writer;
3. write one 1024-byte message;
4. commit the transaction, including the Topic flush.

`BenchmarkTransactionalWriter` covers four fixed-topology scenarios for every
requested partition count:

- `single`: writer without `WithWriteToManyPartitions`;
- `many-key`: `WithWriteToManyPartitions` and `KafkaHashPartitionChooser`;
- `many-bounded-key`: `WithWriteToManyPartitions` and `BoundPartitionChooser`;
- `many-partition-id`: `WithWriteToManyPartitions` and explicit partition IDs.

`BenchmarkTransactionalWriterAutoSplit` exercises the bounded-key writer while
YDB splits a Topic from one active partition toward a maximum of 64.

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
- `retries/tx`: Query transaction retries per logical transaction;
- `StreamWrite/tx`: Topic StreamWrite sessions opened per commit;
- `workers`: effective `GOMAXPROCS` and parallel worker count;
- `measured-s`: duration of the timed region.

A run fails if any logical transaction has a final error. Query retries remain
enabled because that matches normal SDK usage.

The default DSN can also be supplied through `YDB_CONNECTION_STRING`. Use
`-ydb-benchmark-topic-prefix` and `-ydb-benchmark-table` when the default object
names would collide with another run.

## Auto-split scenario

Auto-split has a time-dependent topology, so it deliberately defines one
benchmark operation as one two-minute phase. Run it with `-benchtime=1x`; using
the usual adaptive `b.N` calibration would measure successive, different
topologies.

```bash
go test ./tests/integration/transactional-writer-benchmark \
  -run '^$' \
  -bench '^BenchmarkTransactionalWriterAutoSplit$' \
  -benchtime=1x \
  -count=3 \
  -cpu=4 \
  -args \
  -ydb-benchmark-dsn grpc://localhost:2136/local
```

This benchmark additionally reports the final `active-partitions` and
`ms/first-split`. Because `b.N` is one, its standard `B/op` and `allocs/op`
columns describe the complete two-minute phase; use `tx/s` and latency metrics
for throughput and response-time comparisons.

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
