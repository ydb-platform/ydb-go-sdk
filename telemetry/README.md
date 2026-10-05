# Observable gauges

This experimental package separates observable metrics from diagnostic tracing.
The initial increment provides one int64 gauge:
`ydb.topic.reader.partition_session.count`, with unit `{session}` and sum
reduction. Existing `metrics.Registry`, counters, histograms and traces are
unchanged.

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

Imports in this example are `github.com/ydb-platform/ydb-go-sdk/v3`,
its `telemetry` package and its `topic/topicoptions` package. The application
owns the export cycle and exporter. `Collector` is a functional in-memory pull
backend; it does not run a timer or export to a monitoring service. A native
backend can implement `Meter` directly.

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
ownership, not an atomic view of server assignments. Reconnects read only the current stream and keep one
registration per logical reader/listener.

Sources with equal descriptor and attribute sets are reduced before export.
Attribute order does not change series identity. A failed source suppresses
that descriptor for the collection and returns an error; unrelated descriptors
can still be collected. A backend must preserve these rules rather than emit
several observations for one series.

`Snapshot` must return owned data without I/O or SDK mutation, honor cancellation,
and support concurrent collections. `Registration.Close` detaches a source even
if its context expires, and a subsequent call can await quiescence. Successful
close waits for collections already using that source. Such collections may
return their earlier observations; collections begun after detach cannot use it.
No backend or source call occurs while the collector registry lock is held.

Registration failures return before a reader/listener starts its connection.
Resource close unregisters its source. Driver close unregisters any remaining
sources in its own scope and its child drivers' scopes; it does not take ownership
of readers or an application exporter. A shared meter therefore survives closing
one resource or driver. `WithMeter(nil)` disables this path, including on a child
driver. Metrics do not depend on trace detail settings.

## Runtime complexity

The existing Add/Set gauge contract cannot collect current state on demand or
remove a source safely during collection. Registration tracking and an in-flight
collection count are required to provide those lifetime guarantees; using only a
trace callback would require a second event ledger and could miss current state.
The SDK snapshot reads the actual session storage. One atomic terminal flag is
necessary because listeners replace partition contexts while starting workers;
reading that mutable context from a collector would race. No event bus, sampling
goroutine, per-message metric update, exporter dependency or generated trace
change is introduced.
