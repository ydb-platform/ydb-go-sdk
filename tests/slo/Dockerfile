FROM golang:1.20 as build
ARG SRC_PATH
ARG JOB_NAME
COPY . /src
WORKDIR /src/tests/slo/${SRC_PATH}
RUN CGO_ENABLED=0 go build -o /build/slo-go-workload -ldflags "-X \"main.label=${SRC_PATH}\" -X \"main.jobName=${JOB_NAME}\"" .

FROM scratch
COPY --from=build /build /
ENTRYPOINT ["/slo-go-workload"]
