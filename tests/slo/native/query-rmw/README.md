# Native Query-service transactional RMW

This is a separate SLO executable, not a mode of the ordinary KV workload.
Only the native Query-service adapter is supported. The other KV adapters do
not acquire RMW support through this addition.

From `tests/slo`:

```sh
go build ./native/query-rmw
YDB_ENDPOINT=grpc://ydb:2136 YDB_DATABASE=/Root/testdb \
WORKLOAD_NAME=native-query-rmw WORKLOAD_REF=current WORKLOAD_DURATION=60 \
./query-rmw -rmw-workers=8 -rmw-counters=1 -write-rps=100
```

The existing Dockerfile also accepts `SRC_PATH=native/query-rmw`. The ordinary
SLO workflow has a separate `native-query-rmw` matrix entry and retains its
process-error gate and developer-only `SLO` activation. Its existing overlay
copies this executable and shared internal workload sources into baseline while
retaining the baseline SDK through the local module replacement.
The pull-request trigger also accepts the dependent baseline feature branch,
so the stacked RMW PR can run this same workflow after developer activation.

The existing framework supplies duration, signal handling, logs, OTLP metrics
and the setup/run/teardown lifecycle. Connection, SDK retry budget, session pool
and read/write timeout settings reuse `kv.ParseParams` without changing its
flags or defaults. The new flags are local to this executable:

- `-rmw-workers`: concurrent writers, default 8, minimum 2.
- `-rmw-counters`: counters shared by all writers, default 1, must be positive.

`-write-rps` bounds total logical-operation admission; `-write-timeout` bounds
each admitted transaction, including existing SDK retries. `-read-timeout`
bounds the independent final snapshot. Ordinary KV `-read-rps`, prefill and
partition options have no RMW data-generation meaning; they remain accepted
without changing any ordinary KV executable. Metrics use existing `write` for
logical RMW operations and `read` for the final invariant snapshot; SDK DoTx
attempt counts are reported through the existing metric implementation.

One random process namespace is created before setup and retained for both
counter and ledger table paths through teardown. Writers use shared counters
and monotonically assigned unique logical operation IDs within that namespace.
`query.Client.DoTx` owns begin, serializable read-write isolation (the SDK
default), commit, rollback and retries. Each callback first reads that ID's
ledger record. An exact existing record makes a replay a no-op. Otherwise the
callback reads the counter, increments value/version, then updates the counter
and inserts the ledger record atomically. IDs are captured outside DoTx and do
not change after an ambiguous commit.

Duration expiry or signals stop scheduling, not admitted calls. Those calls
settle under the unchanged write timeout before a fresh transaction reads all
counter and ledger rows. Validation requires exactly the expected ID set,
exact counter/delta payloads, no duplicate or unexpected rows, and each
counter's exact value and version. A final transaction error is returned intact
even if the diagnostic snapshot also fails. The framework turns that error or
an invariant failure into process exit 1.

## Deterministic verification and native proof boundary

```sh
GOTOOLCHAIN=auto GOMAXPROCS=4 go test -race -shuffle=on -count=1 -p 4 ./...
GOTOOLCHAIN=auto GOMAXPROCS=4 go build ./...
```

`internal/rmw` tests invoke the production transaction callback and invariant
checker through a bounded optimistic in-memory transaction seam. They force a
conflict with channel barriers, retry after rollback, persist a transaction
before replaying the same ID, falsify exact ledger/counter state, and stop
scheduling with operations in flight. They do not emulate SDK recovery or prove
real YDB transaction behavior.

Native verification also requires a successful contended run, database-boundary
lost-update/ledger corruption negatives, recovered conflicts, commit-response
loss after persisted commit, and duration shutdown with live transactions. The
ordinary hosted SLO workflow must build both images, export telemetry and enforce
the process verdict. Local unit success does not establish those proofs.
