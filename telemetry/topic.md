# Topic partition-session gauge

`ydb.WithMeter` connects an independent meter to SDK-owned observable sources.
Topic readers and listeners expose `ydb.topic.reader.partition_session.count`,
with unit `{session}` and sum reduction.

```go
meter := telemetry.NewCollector()
db, err := ydb.Open(ctx, connectionString, ydb.WithMeter(meter))
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

// Call from an application collection/export cycle, not a read callback.
metrics, err := meter.Collect(ctx)
```

Imports are `github.com/ydb-platform/ydb-go-sdk/v3`, its `telemetry` package
and its `topic/topicoptions` package. The application owns the export cycle and
exporter. See [observable gauges](README.md) for the backend contract.

Listeners use the same metric and `topicoptions.WithListenerName`. Empty names
use `default`. Attributes are configured endpoint authority, normalized database,
absolute normalized topic path, consumer and reader.name. No partition, session,
connection or reconnect IDs are exposed. Selected topics have zero observations
before admission and after all their sessions retire. A closed resource has no
source or observations.

The count reads SDK-owned admitted sessions, including sessions awaiting start
confirmation or graceful retirement. Retired or explicitly closed sessions and
retained storage tombstones are excluded. Listener retirement preserves the
callback context for a subsequent forced-stop notification. This describes SDK
ownership, not an atomic view of server assignments. Reconnects read only the
current stream and keep one registration per logical reader/listener.

Registration failures return before a reader/listener starts its connection.
Resource close unregisters its source. Driver close unregisters any remaining
sources in its own scope and its child drivers' scopes; it does not take ownership
of readers or an application exporter. A shared meter therefore survives closing
one resource or driver. `WithMeter(nil)` disables this path, including on a child
driver. Metrics do not depend on trace detail settings.

## Runtime complexity

Driver scopes track registrations so closing one driver detaches its sources
without closing a shared application meter. The SDK snapshot reads the actual
session storage; trace deltas would require a second event ledger and could miss
current state. One atomic terminal flag is necessary because listeners replace
partition contexts while starting workers; reading that mutable context from a
collector would race. No event bus, sampling goroutine, per-message metric update,
exporter dependency or generated trace change is introduced.
