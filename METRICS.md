# Topic reader and listener metrics

Readers and listeners share eight instruments. All carry `endpoint`, `database` and `reader.name`. The endpoint is the configured authority and the database is the normalized path. Omitted or empty reader and listener names become `default`. Readers with identical labels share a series.

`consumer` is present only when reading with a consumer.

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

Each queued range counts its half-open span, including gaps. Duplicate submissions count separately. An acknowledgement completes only the fully covered prefix in admission order. Backward acknowledgements add zero. An equal watermark can complete a newly queued range ending there.

Session errors carry the transport or YDB status name and an error type of `transport_error`, `ydb_error` or `unknown`. Unknown gRPC codes use `Code(n)`, unknown YDB codes use their number and unspecified status uses `unknown`. An idle pull reader can first report a stored terminal stream failure as `stop` on close. This check uses only the default retry policy and excludes context errors. It does not invoke a custom retry callback.

Listener hooks use `TopicListenerStreamEvents`. Reader message, byte and gauge hooks use `TopicReaderMessageEvents`. Reader commit and session-error hooks use `TopicReaderStreamEvents`.

Descriptor-aware registries receive the canonical names and units above. Legacy registries build names with `WithSystem` and choose their separator. Their vector API has no unit parameter. Without `Add(int64)`, received bytes and both commit counters skip the increment. Received and delivered messages and session errors fall back to `Inc()` per unit.
