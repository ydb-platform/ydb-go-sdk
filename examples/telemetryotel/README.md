# OpenTelemetry gauges

Create one adapter per OpenTelemetry instrumentation scope and pass that same
adapter to every YDB driver whose gauge contributions should be summed:

```go
adapter := telemetryotel.Meter(otel.Meter("ydb"))
first, err := ydb.Open(ctx, firstDSN, ydb.WithMeter(adapter))
// Handle err before opening the second driver.
second, err := ydb.Open(ctx, secondDSN, ydb.WithMeter(adapter))
// Handle err and close both drivers when finished.
```

The adapter registers one OpenTelemetry callback per active gauge name and sums
observations with the same attributes. If you create it from the global meter
before installing an OpenTelemetry provider, keep using that adapter after
installation.

Calling `telemetryotel.Meter` again creates a separate registry, even for the
same OpenTelemetry meter. Separate callbacks are not summed: if both observe
the same instrument and attribute set, the OpenTelemetry SDK exports the last
observation in collection order.
