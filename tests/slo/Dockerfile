FROM golang:1.24 AS build
ARG SRC_PATH
ARG JOB_NAME
ARG REF=unknown
COPY . /src
WORKDIR /src/tests/slo/${SRC_PATH}
RUN CGO_ENABLED=0 go build -o /build/slo-go-workload -ldflags "-X \"main.ref=${REF}\" -X \"main.label=${SRC_PATH}\" -X \"main.jobName=${JOB_NAME}\"" .

FROM scratch
COPY --from=build /build /
ENTRYPOINT ["/slo-go-workload"]
