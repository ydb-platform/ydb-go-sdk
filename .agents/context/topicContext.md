# Topic service context

> **Load on demand** — only when working on `topic/`, `internal/topic/`, or topic integration tests.  
> Multi-writer specifics: [`topicMultiwriterContext.md`](topicMultiwriterContext.md).

## Public vs internal

| Layer | Path | Notes |
|-------|------|-------|
| Public client | `topic/` (`Client`, `topicoptions`, `topicreader`, `topicwriter`, `topictypes`) | Stable API |
| Wiring | `internal/topic/topicclientinternal/` | `StartReader` / `StartWriter` / admin ops |
| Reader | `internal/topic/topicreaderinternal/` | Reconnect, commits, split/merge |
| Single writer | `internal/topic/topicwriterinternal/` | `WriterReconnector`, queue, reconnect |
| Listener | `internal/topic/topiclistenerinternal/` | Partition workers, routing |
| Trace | `internal/topic/gtrace/` | Generated wrappers |

`Driver.Topic()` → lazy client; no session pool (own stream semantics).

## Writer modes

| Mode | Public option | Internal |
|------|---------------|----------|
| Single partition / classic writer | default `StartWriter` | `topicwriterinternal.WriterReconnector` |
| Many partitions | `topicoptions.WithWriteToManyPartitions(...)` | `internal/topic/topicmultiwriter/` — see [`topicMultiwriterContext.md`](topicMultiwriterContext.md) |

## Integration tests

- Package: `tests/integration/` (`//go:build integration`)
- Helpers: `helpers_test.go`, topic tests `topic_*_test.go`
- Requires local YDB — see [`techContext.md`](techContext.md)

Stress one test: `-count=N -run 'TestName$' ./tests/integration`

## Docs in repo

- [`topic/README.md`](../../topic/README.md) — user-facing overview
