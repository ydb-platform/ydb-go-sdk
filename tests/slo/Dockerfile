FROM golang:1.20 as build
ARG SRC_PATH
COPY . /src
WORKDIR /src/tests/slo/${SRC_PATH}
RUN CGO_ENABLED=0 go build -o /build/slo-go-workload .

FROM scratch
COPY --from=build /build /
ENTRYPOINT ["/slo-go-workload"]
