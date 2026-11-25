# Topic partition-session gauge

Topic readers and listeners expose the `ydb.topic.reader.partition_session.count`
observable gauge in `{session}` units.

```go
db, err := ydb.Open(ctx, connectionString, ydb.WithMeter(telemetryotel.Meter(otelMeter)))
if err != nil {
    return err
}
defer db.Close(ctx)

reader, err := db.Topic().StartReader(
    "consumer",
    topicoptions.ReadTopic("topic"),
    topicoptions.WithReaderName("ingestion"),
)
if err != nil {
    return err
}
defer reader.Close(ctx)
```

The application owns the provider, collection and export. See the
[OpenTelemetry adapter example](../examples/telemetryotel/otel.go).
Different resources can observe the same attribute set, and the backend must
sum their observations within one collection.

| Attribute | Value |
| --- | --- |
| `reader.name` | Resource name (`default` when empty). Listeners use `topicoptions.WithListenerReaderName`. |
| `topic` | Absolute normalized path of the selected topic. |
| `consumer` | Consumer name. Omitted for consumerless reads. |
| `endpoint` | Configured endpoint authority. |
| `database` | Normalized database path. |

The gauge counts SDK-owned sessions on the current stream from admission until
the SDK closes the session locally. On a graceful stop the session is closed
right before the SDK sends the stop response; a listener first waits for the
handler to confirm the stop and flushes pending commits. Forced stops retire
sessions immediately. Selected
topics with no active sessions report zero until the resource closes.

Close readers and listeners before closing their driver. Close disables new
observations and schedules unregister without waiting for the backend, so it
can run during or from inside a collection. An in-flight callback may finish.
