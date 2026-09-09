# Topic multiwriter context

> **Load on demand** — only for `WithWriteToManyPartitions`, `internal/topic/topicmultiwriter/`, or multiwriter integration tests.
> General topic layout: [`topicContext.md`](topicContext.md).

## Entry points

- Public: `topic.Client.StartWriter(path, topicoptions.WithWriteToManyPartitions(opts...))`
- Internal: `topicmultiwriter.NewMultiWriter` → `MultiWriter` + `orchestrator`

Key files:

| File | Role |
|------|------|
| `topicmultiwriter.go` | Public `MultiWriter` API, starts init goroutine |
| `orchestrator.go` | Init, seq baseline, split handling, sender coordination |
| `partition_writer_pool.go` | Per-partition writers (direct / non-direct) |
| `sender.go` | Drains inflight buffer to partition writers |
| `partition_split_receiver.go` | Serializes split events |

## Init lifecycle (order matters)

```
NewMultiWriter → background goroutine runs orchestrator.init()
  1. Describe topic → populate partitions map
  2. initSeqNo() → getMaxSeqNo() obtains partition writers and waits for init info, then evicts them
  3. Register partitions in PartitionChooser
  4. startWorkers() → ack receiver, partition splitter, sender
```

`WaitInit()` blocks on `initDone` (closed when `init()` returns).

## Invariant

Message-pipeline workers (splitter / sender / ack) must start only after steps 1–3 succeed.

If `onPartitionSplit` runs during `initSeqNo`, it can `evict()` writers still used by `getMaxSeqNo` → `WaitInit` fails with `ydb: stop writer reconnector`.

Split events pushed before `startWorkers()` stay queued in `partitionSplitReceiver` and run after init.

## Tests

| Test | Package | Role |
|------|---------|------|
| `TestMultiWriter_WaitInit_PartitionSplitQueuedDuringInit` | `internal/topic/topicmultiwriter` | Unit regression for init vs split race |
| `TestTopicMultiWriter_AutoPartitioning_SplitDuringInFlightBatch` | `tests/integration` | Auto-partitioning stress; was flaky on `ydbplatform/local-ydb:latest` |

Reproduce integration flakiness:

```bash
go test -race -tags integration -count=50 \
  -run 'TestTopicMultiWriter_AutoPartitioning_SplitDuringInFlightBatch$' \
  ./tests/integration
```

Unit tests: `go test -race ./internal/topic/topicmultiwriter/...`
