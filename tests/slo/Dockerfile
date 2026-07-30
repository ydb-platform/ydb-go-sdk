FROM golang:1.26 AS build

# Copy dependency manifests first so module download is cached when only source changes.
COPY go.mod go.sum /src/
COPY tests/slo/go.mod tests/slo/go.sum /src/tests/slo/

WORKDIR /src/tests/slo
# Avoid intermittent HTTP/2 stream errors between CI runners and the Go module proxy.
RUN set -eu; \
    for attempt in 1 2 3 4 5; do \
        GODEBUG=http2client=0 go mod download && exit 0; \
        if [ "$attempt" -eq 5 ]; then \
            exit 1; \
        fi; \
        echo "go mod download failed (attempt $attempt/5), retrying..." >&2; \
        sleep $((attempt * 5)); \
    done

ARG SRC_PATH
COPY . /src

WORKDIR /src/tests/slo/${SRC_PATH}
RUN CGO_ENABLED=0 go build -o /build/slo-go-workload .

FROM scratch
COPY --from=build /build /
ENTRYPOINT ["/slo-go-workload"]
