# YDB server-side tracing (OpenTelemetry)

This folder keeps a **custom YDB config** that enables server-side OpenTelemetry tracing for the demo.

## 1) Export the default config from a running container

If YDB is running as `ydb-local`:

```bash
docker cp ydb-local:/ydb_data/cluster/kikimr_configs/config.yaml ./ydb_config/ydb-config.yaml
```

## 2) Enable OpenTelemetry exporter in the config

Edit `ydb-config.yaml` and add the contents of `otel-tracing-snippet.yaml` (usually as a top-level section).

Default OTLP endpoint (inside docker-compose network): `grpc://otel-collector:4317`
Default service name (so you can find it in Tempo/Grafana): `ydb`

## 3) Run with the overridden config

Restart YDB (the main `compose-e2e.yaml` passes `--config-path /ydb_config/ydb-config.yaml`):

```bash
docker-compose -f compose-e2e.yaml up -d --force-recreate ydb
```

Now you should see additional server-side traces in Tempo/Grafana under service name `ydb`.
