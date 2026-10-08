# Topic partition-session gauge

`ydb.WithMeter` supplies a function that registers SDK callbacks in a backend.
Topic readers and listeners expose `ydb.topic.reader.partition_session.count`,
with unit `{session}`.

```go
// Wrap an application-configured OTel meter with the example adapter.
meter := telemetryotel.Meter(otelMeter)
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

// The application's OTel reader/exporter invokes the registered callback.
```

`telemetryotel.Meter` is defined in [the adapter example](../examples/telemetryotel/otel.go).
The application owns the OTel provider, collection and export. The SDK does not
import OTel or maintain a collector. Other backends implement the same function
contract; see [the callback API](telemetry.go).

For Prometheus, use [the Prometheus example](../examples/telemetryprometheus/prometheus.go)
with an application-owned registry:

```go
registry := prometheus.NewRegistry()
meter := telemetryprometheus.Meter(ctx, registry)
db, err := ydb.Open(ctx, connectionString, ydb.WithMeter(meter))
// Handle err and close readers/listeners before db as above.
handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
// Serve handler at the application's /metrics endpoint.
```

This example supports the fixed attribute sets of topic resources. Registration
reads their labels once, then every scrape reads current counts. Metric and label
names replace dots with underscores, including
`ydb_topic_reader_partition_session_count` and `reader_name`. The backend-local
Collector and lock are needed by Prometheus registration and concurrent
unregister; no metric values are cached. Neither `ydb-go-sdk-otel` nor
`ydb-go-sdk-prometheus` is required for these callbacks. Those packages remain
useful for the existing trace/Registry metrics and can share the application's
backend with the new adapter.

Listeners use the same metric and `topicoptions.WithListenerName`. Empty names
use `default`. Attributes are configured endpoint authority, normalized database,
absolute normalized topic path, consumer and reader.name. No partition, session,
connection or reconnect IDs are exposed. Selected topics have zero observations
before admission and after all their sessions retire. A closed resource has no
registered callback.

Use distinct reader/listener names for independent resources. Native gauges do
not sum observations with equal labels; any aggregation is backend policy.

The count reads SDK-owned admitted sessions, including sessions awaiting start
confirmation or graceful retirement. Retired or explicitly closed sessions and
retained storage tombstones are excluded. Listener retirement preserves the
callback context for a subsequent forced-stop notification. This describes SDK
ownership, not an atomic view of server assignments. Reconnects read only the
current stream and keep one registration per logical reader/listener.

Registration failures return before a reader/listener starts its connection.
Resource close invokes the backend's unregister function before releasing its
state. Close readers/listeners before their driver: a driver does not own or
track their callbacks. Child drivers inherit the registration function, unless
overridden with `WithMeter(nil)`. Neither resource nor driver close shuts down the
shared application backend. Metrics do not depend on trace detail settings.

## Runtime complexity

Registration directly calls the application function; the SDK retains only the
resource's unregister function. The callback reads the actual session storage;
trace deltas would require a second event ledger and could miss
current state. One atomic terminal flag is necessary because listeners replace
partition contexts while starting workers; reading that mutable context from a
callback would race. There is no telemetry registry, reduction state, event bus,
sampling goroutine, per-message update, exporter dependency or generated trace change.
