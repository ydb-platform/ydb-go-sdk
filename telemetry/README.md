# Observable gauges

This experimental package is a registration contract, not a metrics backend.
`Meter` is a function that immediately registers an observable gauge callback
with the application's backend and returns that backend's unregister function.
No interfaces or source implementation structs are required.

```go
unregister, err := meter(telemetry.Descriptor{
    Name: "application.workers", Unit: "{worker}",
}, func(ctx context.Context, observe func(int64, ...telemetry.Attribute)) error {
    if err := ctx.Err(); err != nil {
        return err
    }
    observe(workers.Load())
    return nil
})
if err != nil {
    return err
}
// The backend invokes the callback in its own collection cycle.
// Unregister before releasing the resource read by the callback.
return unregister()
```

`workers` can be an application-owned `atomic.Int64`. The callback reads current
state and calls `observe`; it must not perform I/O, mutate SDK state or retain the
observer. Concurrent collections must be safe.

The backend owns instruments, collection, aggregation and unregister
synchronization. A successful registration returns a non-nil, idempotent
unregister function; failed registration must not retain the callback. The SDK
does not maintain a collector, registration registry, in-flight counters, sample
cache, timer or exporter. Existing `metrics.Registry` and tracing are unchanged.

Gauges are non-additive: equal labels do not imply sum or max. Use distinct
attributes for independent resources, or configure aggregation in the backend.

See [the OTel adapter](../examples/telemetryotel/otel.go): it directly creates
an `Int64ObservableGauge`, registers the callback and returns native
`Registration.Unregister`. It uses the examples module's existing dependencies;
the core SDK does not import OTel. An adapter for Monium or another backend follows
the same function contract and translates attributes to that backend's API.
