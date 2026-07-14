FROM golang:1.26 AS build
ARG SRC_PATH
ARG ENABLE_COVERAGE=false

# Copy dependency manifests first so module download is cached when only source changes.
COPY go.mod go.sum /src/
COPY tests/slo/go.mod tests/slo/go.sum /src/tests/slo/

WORKDIR /src/tests/slo
RUN go mod download

COPY . /src

WORKDIR /src/tests/slo/${SRC_PATH}
RUN if [ "$ENABLE_COVERAGE" = "true" ]; then \
      CGO_ENABLED=0 go build -trimpath \
        -cover -coverpkg=github.com/ydb-platform/ydb-go-sdk/v3/... \
        -o /build/slo-go-workload .; \
    else \
      CGO_ENABLED=0 go build -trimpath -o /build/slo-go-workload .; \
    fi

FROM scratch
COPY --from=build /build /
ENV GOCOVERDIR=/coverage
ENTRYPOINT ["/slo-go-workload"]
