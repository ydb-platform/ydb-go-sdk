FROM golang:1.20 as build
ADD . /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -o /build/slo-go-workload native/main.go

FROM scratch
COPY --from=build /build /
ENTRYPOINT ["/slo-go-workload"]
