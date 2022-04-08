## Ecosystem of debug tools over `ydb-go-sdk` <a name="Debug"></a>

Package `ydb-go-sdk` provide debugging over trace events in package `trace`.
Now supports driver events in `trace.Driver` struct and table-service events in `trace.Table` struct.
Next packages provide debug tooling:

Package | Type | Description                                                                                                                                                                                 | Link of example usage
--- | --- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ---
[ydb-go-sdk-zap](https://github.com/ydb-platform/ydb-go-sdk-zap) | logging | logging ydb-go-sdk events with zap package                                                                                                                                                  | [ydbZap.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-zap/blob/master/internal/cmd/bench/main.go#L64)
[ydb-go-sdk-zerolog](https://github.com/ydb-platform/ydb-go-sdk-zap) | logging | logging ydb-go-sdk events with zerolog package                                                                                                                                              | [ydbZerolog.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-zerolog/blob/master/internal/cmd/bench/main.go#L47)
[ydb-go-sdk-metrics](https://github.com/ydb-platform/ydb-go-sdk-metrics) | metrics | common metrics of ydb-go-sdk. Package declare interfaces such as `Registry`, `GaugeVec` and `Gauge` and use it for create `trace.Driver` and `trace.Table` traces                           |
[ydb-go-sdk-prometheus](https://github.com/ydb-platform/ydb-go-sdk-prometheus) | metrics | prometheus wrapper over [ydb-go-sdk-metrics](https://github.com/ydb-platform/ydb-go-sdk-metrics) | [ydbPrometheus.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-prometheus/blob/master/internal/cmd/bench/main.go#L56)
[ydb-go-sdk-opentracing](https://github.com/ydb-platform/ydb-go-sdk-opentracing) | tracing | opentracing plugin for trace internal ydb-go-sdk calls | [ydbOpentracing.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-opentracing/blob/master/internal/cmd/bench/main.go#L86)
