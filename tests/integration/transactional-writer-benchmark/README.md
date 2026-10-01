# Transactional Topic writer benchmark

This package contains standard Go benchmarks for a transactional-outbox path:

1. create a transactional Topic writer while the Query transaction is lazy;
2. execute one `UPSERT` through the Query API, materializing the transaction;
3. write one 1024-byte message through the already-created writer;
4. commit the transaction, including the Topic flush.

Every attempt uses `query.WithLazyTx(true)`. Creating the writer before the
`UPSERT` exercises the transition from the lazy transaction ID to the materialized
transaction before the writer calls `UnLazy` during `Write`.

Messages leave `SeqNo` unset. The writer's default automatic numbering is used,
so the benchmark does not generate or measure user-provided sequence numbers.

`BenchmarkTransactionalWriter` covers three fixed-topology scenarios for every
requested partition count:

- `single`: writer without `WithWriteToManyPartitions`;
- `many-key`: `WithWriteToManyPartitions` and `KafkaHashPartitionChooser`;
- `many-bounded-key`: `WithWriteToManyPartitions` and `BoundPartitionChooser`.

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

The standard `ns/op` column is accompanied by only:

- `streams/tx`: Topic StreamWrite sessions opened per commit;
- `errors`: final transaction error count.

The auto-split benchmark also reports `partitions` after the measurement.
Latency distributions, retry counts, worker counts, and redundant throughput
or duration columns are deliberately omitted to keep both the output and the
measured path small.

Final transaction errors are recorded but do not stop or fail the measurement.
Setup and connection errors still fail the benchmark. Query retries remain
enabled because that matches normal SDK usage.

The default DSN can also be supplied through `YDB_CONNECTION_STRING`. Use
`-ydb-benchmark-topic-prefix` and `-ydb-benchmark-table` when the default object
names would collide with another run.

## Auto-split scenario

As in the fixed-topology benchmark, one benchmark operation is one transaction.
Use a fixed operation count so Go does not calibrate `b.N` while the Topic
topology is changing. The baseline uses 300 transactions, which gives the
Topic enough sustained load to split. Restart the YDB container before each
reported repetition so prior splits and StreamWrite sessions cannot affect it.

```bash
go test ./tests/integration/transactional-writer-benchmark \
  -run '^$' \
  -bench '^BenchmarkTransactionalWriterAutoSplit$' \
  -benchtime=300x \
  -count=1 \
  -cpu=4 \
  -args \
  -ydb-benchmark-dsn grpc://localhost:2136/local
```

Repeat this command three times, restarting the YDB container before each run.

This benchmark describes the Topic once before the measurement and once after
it, then reports the final `partitions`.

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
master baseline in standard Go benchmark format and its exact environment are
recorded in a comment near the benchmark implementation so a later PR can
compare against the same protocol.
