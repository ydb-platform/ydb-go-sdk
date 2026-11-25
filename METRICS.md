# Topic reader and listener metrics

Readers and listeners share eight instruments. All carry `endpoint`, `database` and `reader.name`. The endpoint is the configured authority and the database is the normalized path. Use `topicoptions.WithReaderName` for pull readers and `topicoptions.WithListenerReaderName` for listeners. Omitted or empty reader and listener names become `default`. Readers with identical labels share a series.

Each instrument declares one label schema. Without a consumer, legacy registries receive `consumer=""` so their fixed-label vectors can bind the series. The contract form (no `consumer` key) requires a descriptor-aware registry: `With` may receive a subset of the declared names, and an omitted name means the attribute is absent.

| Instrument | Unit | Additional labels | What it measures |
| --- | --- | --- | --- |
| `ydb.topic.reader.received.messages` | `{message}` | topic | Messages accepted from the stream |
| `ydb.topic.reader.delivered.messages` | `{message}` | topic | Messages handed to the application |
| `ydb.topic.reader.received.bytes` | `By` | — | Protocol size of each read response, including discarded data |
| `ydb.topic.reader.session.errors` | `{error}` | retry_decision, status_code, error.type | Retry and stop decisions for failed sessions |
| `ydb.topic.reader.commit.queued` | `{message}` | topic | Sum of accepted non-transactional commit spans |
| `ydb.topic.reader.commit.acknowledged` | `{message}` | topic | Sum of completed queued spans |
| `ydb.topic.reader.local_buffer.messages` | `{message}` | — | Messages currently held across all topics of the reader |
| `ydb.topic.reader.credit_balance_bytes` | `By` | — | Bytes requested successfully minus bytes received |

The first six instruments are cumulative counters. The two gauges apply signed deltas in stream order and compensate the outstanding balance on close.

Each queued range counts its half-open span, including gaps. Duplicate submissions count separately. Ranges ending at or below the committed watermark are excluded from both counters. An acknowledgement completes every fully covered pending range, regardless of admission order. Equal and backward acknowledgements add zero. Repeated submissions still pending when the watermark advances each count their full span.

Session errors carry the transport or YDB status name and an error type of `transport_error`, `ydb_error` or `unknown`. Unknown gRPC codes use `Code(n)`, unknown YDB codes use their number and unspecified status uses `unknown`. An idle pull reader can first report a stored terminal stream failure as `stop` on close. This check uses only the default retry policy and excludes context errors. It does not invoke a custom retry callback.

Listener hooks use `TopicListenerStreamEvents`. Reader message, byte and gauge hooks use `TopicReaderMessageEvents`. Reader commit and session-error hooks use `TopicReaderStreamEvents`.

The detail mask is read once, when the topic trace is built: a later change of a live `trace.Detailer` does not affect topic metrics, so a gauge delta and its compensation always follow the same mask. A disabled group installs no hook, and the SDK does no bookkeeping for an absent hook. Pending commits are tracked per reader kind: `OnReaderCommitQueued` enables it for pull readers and `OnListenerCommitQueued` for listeners.

Descriptor-aware registries receive the canonical names and units above. Legacy registries build names with `WithSystem` and choose their separator. Their vector API has no unit parameter. The five bulk counters (received messages, delivered messages, received bytes, queued commits and acknowledged commits) use one `Add(int64)` per positive update, or one `Add(float64)` if that is the available method. The released Prometheus adapter v2.1.2 exposes `Add(float64)`. An Inc-only counter receives no bulk updates, even when the delta is one: publishing only singleton batches would undercount. `session.errors` always calls `Inc()` once per decision.

The `ydb-go-sdk-otel` v0.11.1 counter implements only `Inc()`; it needs `Add(int64)` before these five bulk counters produce data there. The adapter must also implement the descriptor interfaces to export canonical names and units on a real Meter without renaming legacy metrics.
