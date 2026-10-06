# Observable gauges

This experimental package provides backend-independent observable int64 gauges.
It is independent of diagnostic tracing and does not register SDK metrics.
Existing `metrics.Registry`, counters, histograms and traces are unchanged.

An application implements `Int64GaugeSource` and registers it with a `Meter`.
`Collector` is an in-memory pull backend; a native backend can implement `Meter`
directly. The application owns collection and export.

```go
type source struct {
    value atomic.Int64
}

func (s *source) Snapshot(ctx context.Context) ([]telemetry.Int64Point, error) {
    return []telemetry.Int64Point{{Value: s.value.Load()}}, ctx.Err()
}
```

```go
meter := telemetry.NewCollector()
state := new(source)
state.value.Store(3)
reg, err := meter.RegisterInt64Gauge(telemetry.Int64GaugeDescriptor{
    Descriptor: telemetry.Descriptor{Name: "application.workers", Unit: "{worker}"},
    Reduction: telemetry.GaugeSum,
}, state)
if err != nil {
    return err
}
metrics, collectErr := meter.Collect(ctx)
closeErr := reg.Close(ctx)
return errors.Join(collectErr, closeErr)
```

Imports are `context`, `errors`, `sync/atomic` and
`github.com/ydb-platform/ydb-go-sdk/v3/telemetry`. Use `metrics` in the
application's export cycle before returning. `GaugeSum` adds values for equal
descriptor and attribute sets; `GaugeMax` selects their maximum. Attribute order
does not change series identity. A failed source suppresses its entire descriptor
for that collection and returns an error; unrelated descriptors remain available.
A native backend must preserve these reduction and failure rules.

`Snapshot` returns owned data without I/O or SDK mutation, honors cancellation
and supports concurrent calls. Registration failure must not retain a source.
`Registration.Close` is idempotent and detaches the source even if its context
expires. A later close can wait for quiescence. Successful close waits for
collections already using the source; those collections may return earlier
observations. Collections begun after detach cannot use it.

Registration tracking and an in-flight count are required to detach sources
safely while collection is running; an Add/Set gauge alone cannot provide those
lifetime guarantees. Source calls happen outside the registry lock. There is no
sampling goroutine, timer, exporter dependency or event ledger.

For SDK-owned topic sources, see [the topic partition-session gauge](topic.md).
