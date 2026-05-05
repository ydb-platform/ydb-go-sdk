# Multi-stage build for the OTel demo program.
#
# The whole point of running the demo inside Docker is to make sure the Go
# app, the YDB server and the OTel collector all share the same wall clock
# (the host clock projected into the containers). When the Go app runs on
# the host while YDB and the collector run in containers, OS-level time
# offsets and timezone differences make span timelines in Tempo/Grafana
# misleading.
#
# Build:
#   docker build -f examples/opentelemetry/Dockerfile -t ydb-otel-sample ../..
#
# Or just `docker compose -f compose-e2e.yaml up --build`.

# ---- build stage --------------------------------------------------------
FROM golang:1.24-alpine AS build

WORKDIR /src

# The example is a sub-module that depends on the parent SDK via a
# `replace` directive (../..). Copy both the parent module and the example
# so `go build` can resolve the local replacement.
COPY go.mod go.sum ./
COPY internal ./internal
COPY pkg ./pkg
COPY trace ./trace
COPY retry ./retry
COPY spans ./spans
COPY meta ./meta
COPY query ./query
COPY balancers ./balancers
COPY config ./config
COPY operation ./operation
COPY scheme ./scheme
COPY scripting ./scripting
COPY sugar ./sugar
COPY table ./table
COPY topic ./topic
COPY ratelimiter ./ratelimiter
COPY discovery ./discovery
COPY credentials ./credentials
COPY coordination ./coordination
COPY log ./log
COPY metrics ./metrics
COPY *.go ./

COPY examples/opentelemetry ./examples/opentelemetry

WORKDIR /src/examples/opentelemetry

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    GOFLAGS="-trimpath" CGO_ENABLED=0 go build -o /out/sample .

# ---- runtime stage ------------------------------------------------------
FROM alpine:3.20
RUN apk add --no-cache tzdata ca-certificates
ENV TZ=UTC
WORKDIR /app
COPY --from=build /out/sample /app/sample
ENTRYPOINT ["/app/sample"]
