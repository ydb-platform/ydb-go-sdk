# Topic reader and listener metrics

The experimental metrics package exposes the following instruments for topic
readers. Topic listener events use the same instruments and names, with
listener callbacks enabled by `TopicListenerStreamEvents`. Reader callbacks
are selected by the corresponding `TopicReaderMessageEvents` or
`TopicReaderStreamEvents` detail group.

## Instruments

| Canonical name | Type | Unit | Attributes | Semantics |
| --- | --- | --- | --- | --- |
| `ydb.topic.reader.received.messages` | counter | `{message}` | `endpoint`, `database`, `topic`, `consumer`, `reader.name` | Messages accepted from the topic stream |
| `ydb.topic.reader.delivered.messages` | counter | `{message}` | `endpoint`, `database`, `topic`, `consumer`, `reader.name` | Messages delivered to the application |
| `ydb.topic.reader.received.bytes` | counter | `By` | `endpoint`, `database`, `consumer`, `reader.name` | Each `ReadResponse.BytesSize`, including discarded or unknown-partition data |
| `ydb.topic.reader.session.errors` | counter | `{error}` | `endpoint`, `database`, `consumer`, `reader.name`, `retry_decision`, `status_code`, `error.type` | Retry or stop decisions for failed sessions |
| `ydb.topic.reader.commit.queued` | counter | `{message}` | `endpoint`, `database`, `topic`, `consumer`, `reader.name` | Accepted commit-range spans |
| `ydb.topic.reader.commit.acknowledged` | counter | `{message}` | `endpoint`, `database`, `topic`, `consumer`, `reader.name` | Fully covered queued commit-range spans, counted once in admission order |
| `ydb.topic.reader.local_buffer.messages` | gauge | `{message}` | `endpoint`, `database`, `topic`, `consumer`, `reader.name` | Accepted messages held by the SDK until delivery, discard, or close |
| `ydb.topic.reader.credit_balance_bytes` | gauge | `By` | `endpoint`, `database`, `consumer`, `reader.name` | Successful read-request bytes minus response bytes, with remaining balance compensated on close |

Absent optional values are emitted as empty strings. `endpoint` is the
configured endpoint authority, and `database` is the normalized configured
database path. The `reader.name` attribute comes from `WithReaderName` or
`WithListenerName`; omitted and empty names default to `default`, so readers
and listeners with the same other attributes share a series. Explicit names,
including `default`, are preserved. The shared stream attributes intentionally
omit `topic`.

Message counters are emitted at their event boundaries: `received.messages`
counts messages accepted from the stream, `delivered.messages` counts messages
handed to the application, and `received.bytes` counts each response's
protocol `BytesSize` once, including discarded or unknown-partition data.
Session-error `retry_decision` records the retry or stop decision. Its
`status_code` uses the transport or YDB status name; unknown gRPC codes use
`Code(n)`, unknown YDB codes use their numeric value, and unspecified or
unclassified errors use `unknown`. `error.type` is `transport_error`,
`ydb_error`, or `unknown`.

An initialized pull reader that becomes idle after its raw stream fails does
not emit a session-error event immediately. A later `Read` handles the failure
through the normal retry flow. If the caller closes the reader first, `Close`
emits a stored stream error as a `stop` only when
`topicoptions.WithReaderCheckRetryErrorFunction` is not configured and the
default retry policy classifies the error as terminal. This close-time check
does not invoke a custom retry callback; default-retryable errors and Go
context cancellation or deadline errors do not produce a close-time `stop`
event.

## Commit ranges

Commit metrics count half-open `[start, end)` spans. Offset gaps contribute to
the span length. Repeated and overlapping submissions are admitted as separate
queued spans. An acknowledgement can fully cover earlier queued spans while
only partially covering a later one; only fully covered spans count, in
admission order. A backward acknowledgement always adds zero. A repeated or
equal acknowledgement adds zero unless a newly admitted range ending at the
current watermark is fully covered; stale acknowledgements add only newly
completed spans. Transactional commits are excluded.

## Interpreting gauges and counters

Counters are cumulative; derive throughput and error rates from changes over a
time window in the monitoring system. Gauge values are maintained with signed
`Gauge.Add` deltas rather than `Gauge.Set`, so adapters can map them to
UpDownCounter instruments: `local_buffer.messages` represents the number of
messages currently held by the SDK, while `credit_balance_bytes` represents
the current read-ahead byte balance. Concurrent delivery and close callbacks
retain the order of these balance deltas.

## Registry compatibility

Descriptor-aware registries implementing `RegistryWithDescriptors` and/or
`RegistryWithGaugeDescriptors` receive the canonical names and explicit units
listed above. Older registries receive names through `WithSystem`,
`CounterVec`, and `GaugeVec`; the separator and exact spelling are chosen by
the registry adapter (an adapter using `_` emits names such as
`ydb_topic_reader_received_messages`), and those interfaces cannot receive
units. The canonical names and units are the metric contract, while legacy
names are compatibility adapter behavior.

`received.bytes`, `commit.queued`, and `commit.acknowledged` require
`Add(int64)` for a batch increment: an Inc-only adapter suppresses a positive
sample rather than expanding it into unit increments. `received.messages`,
`delivered.messages`, and `session.errors` retain the existing `Inc()` fallback
for adapters without `Add(int64)`. The [`ydb-go-sdk-otel` v0.11.1
adapter](https://github.com/ydb-platform/ydb-go-sdk-otel/tree/v0.11.1)
therefore suppresses samples for the required-`Add` counters, while message
and session-error counters remain compatible with `Inc()`-only adapters. That
adapter also lacks descriptor-aware vector creation, so canonical names and
units still require the optional descriptor interfaces. An unsupported sample
does not remove the instrument: an adapter may still register it and expose a
zero or empty series. That is not a startup failure or evidence of zero
traffic.
