FROM golang:1.26 AS build
ARG SRC_PATH

# Copy dependency manifests first so module download is cached when only source changes.
COPY go.mod go.sum /src/
COPY tests/slo/go.mod tests/slo/go.sum /src/tests/slo/

WORKDIR /src/tests/slo
RUN go mod download

COPY . /src

WORKDIR /src/tests/slo/${SRC_PATH}
RUN CGO_ENABLED=0 go build -o /build/slo-go-workload .

FROM scratch
COPY --from=build /build /
ENTRYPOINT ["/slo-go-workload"]
