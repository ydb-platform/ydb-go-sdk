# YDB server research tests

This nested Go module contains executable experiments for YDB server behavior observable through public gRPC protocols.
Research is grouped by service; the current suite covers [Topics](topic/features/research/).

Research scenarios live in a `research/` subdirectory. They record behavior without asserting an expected server response. A run ends
with `RECORDED` when the experiment and its observation machinery worked, or `ERROR` when the experiment itself could not be
performed. It never reports `PASS` or `FAIL` for a server outcome.

Each scenario has an adjacent `# Observed on YDB ...` comment containing the current short conclusion and the exact server
version on which it was observed. These comments are dated evidence, not assertions; update them after intentionally rerunning
the scenario against another server version.

The full suite (21 scenarios in 10 feature files) was recorded with the race detector on 2026-09-08 against
`ydbplatform/local-ydb:trunk`, reporting `main.7f40cb4` through the YDB API. The image digest was
`sha256:7437fab163ffcc3594d3f2498d6e2888cc31928d3d5a00f7372a2d8455408789`.
Query transaction experiments use serializable read-write isolation; they do not compare isolation levels or test SDK pooling.

## Interactive runner

The runner needs Go and Docker Compose v2. Start it from this module:

```bash
go run ./cmd/server-contracts
```

It asks for a YDB Docker image tag and then shows the directory tree containing the tests. Each folder is a submenu; each
`.feature` file is a runnable item named exactly after its `Feature:` title, with the filename in parentheses. Selecting a file
runs **all scenarios in that file**. Every menu has a `0` choice to return one level up; from version selection it exits.
After a run, including an unsuccessful one, the runner returns to the same folder with the selected YDB version unchanged.

The CLI is organized into menu/catalog code, Compose lifecycle management, server version discovery, and test output streaming.
The nested module uses the SDK checkout via a local `replace`; it does not change the SDK's public API or dependencies.

For every run the utility:

1. writes a temporary `compose.yaml` and chooses unused local gRPC and monitoring ports;
2. starts `ydbplatform/local-ydb:<tag>` with `docker compose up --wait`;
3. prints the version reported by YDB itself, the requested image tag, immutable image digest or ID, and image revision metadata;
4. runs only the selected feature file;
5. always calls `docker compose down --volumes --remove-orphans`.

If cleanup fails, the run reports `ERROR` and retains the Compose file and project name for a manual retry.

List or run a feature non-interactively (the path is relative to this module):

```bash
go run ./cmd/server-contracts -list
go run ./cmd/server-contracts \
  -version trunk \
  -test topic/features/research/concurrent_query_transactions.feature
```

### Test discovery

At startup the runner finds `features/` directories under Go packages and recursively reads their `.feature` files using the
Gherkin parser. There is no test registry, generated ID, category list, or tag filter. The menu mirrors the actual directory
structure, including arbitrary nesting. Only folders containing feature files are shown.

For example, adding `topic/features/research/ordering/new_case.feature` adds the `ordering/` submenu and a runnable feature.
Adding `topic/features/contracts/new_case.feature` adds a sibling `contracts/` submenu. Restart the utility to reload the catalog.
Tags are optional metadata and do not determine where a test appears or whether it runs. Existing observations and research
behavior are unchanged; no contract scenarios are provided yet.

A new feature can use the existing Go steps without code changes. A new service needs a Go test package with a
`TestServerFeatures` entry point that honors `YDB_SERVER_FEATURE_PATH`, plus that service's protocol steps.

## Direct run

With an already running YDB:

```bash
YDB_CONNECTION_STRING=grpc://localhost:2136/local go test -v -run '^TestServerFeatures$' ./topic
```

Set `YDB_SERVER_FEATURE_PATH=features/research/concurrent_query_transactions.feature` to run one file; without it the package
runs all files recursively under `features/`.

The module also reads `YDB_ACCESS_TOKEN_CREDENTIALS` and `YDB_SSL_ROOT_CERTIFICATES_FILE`; both `grpc://` and `grpcs://`
connection strings are supported. TLS uses normal certificate verification; the certificate file adds a custom CA.
`YDB_TOPIC_RESEARCH_FORMAT` can select another Godog formatter.

Run the runner's unit tests and static checks without starting YDB:

```bash
go test -race -skip '^TestServerFeatures$' ./...
go vet ./...
golangci-lint run ./...
```

## Output and implementation boundary

Before execution, the runner prints the complete selected scenario. The subsequent `Live gRPC exchange (observed order)` block
contains only the real-time protocol timeline. Full gRPC method paths and protobuf message names are shown; real session and
transaction IDs remain visible alongside stable aliases such as `Transaction A`.

`StreamWrite` uses separate send and receive goroutines. The shared event recorder numbers Query and Topic events in the order in
which those goroutines and gRPC interceptors observe them. Raw `StreamRead` is used only in ordering experiments because topic
metadata exposes offsets but not message payload order. Creating fixtures and obtaining Query transactions uses the Go SDK, but
the subject of every experiment is the server protocol: no SDK behavior is asserted or characterized.

Each read stream has one receiver for its whole lifetime, including across idle observation windows. Cleanup cancels and joins
the receiver. Topic names are unique per run, and cleanup only drops topics successfully created by that scenario.
Live event order is client-observed order, not a claim about the server's internal scheduling.

## Adding a scenario

The fixture step `* an empty topic` creates one partition by default. Use `* an empty topic with 2 partitions` to set both
minimum and maximum active partition counts. It can also include `with consumer "reader" for observation` when reading payloads
is needed. `DescribeTopic` prints the statistics of all returned partitions, including before a write session is opened.

Use Gherkin's neutral `*` step keyword and describe only controlled client actions with actual gRPC method and protobuf message
names. Do not describe expected responses: the live trace prints whatever the server returns. `Recv` is implicit; keep
`CloseSend` explicit only when closing a stream is part of the experiment.

Place the current observation immediately before the scenario:

```gherkin
# Observed on YDB main.abcdef0: seq_no=1 is acknowledged as written.
Scenario: Write one message
```

Compose StreamWrite experiments from the parameterized steps in `topic/stream_write_steps_test.go`. Write `InitRequest` as a
compact protobuf-like object and omit fields that are not set:

```gherkin
* TopicService.StreamWrite: InitRequest{producer_id: research-producer, get_last_seq_no: true}
```

Leaving the `partitioning` oneof unset differs from explicitly setting `partition_id=0`, even though `GetPartitionId()` returns
zero in both cases. Scenarios concerned with ordering therefore set `partition_id` explicitly.

To keep multiple StreamWrite sessions open, give each one a client-side alias:

```gherkin
* TopicService.StreamWrite "A": InitRequest{producer_id: producer, partition_id: 0}
* TopicService.StreamWrite "B": InitRequest{producer_id: producer, partition_id: 0}
* TopicService.StreamWrite "A": WriteRequest messages:
  | data      | seq_no |
  | message-a | 1      |
* TopicService.StreamWrite "B": CloseSend
```

Each named step addresses only that stream. Opening B does not close A on the client; any server-side termination remains
observable. Each stream has independent send/receive goroutines and a response queue, so its live trace continues even while
the scenario addresses another stream. All streams share one numbered event timeline; labels such as `#1 [A]` accompany the
real server session IDs. An alias can be reused after its explicit `CloseSend`, with a new stream number.

Steps without an alias still address a separate default stream, not the most recently opened named stream. Cleanup closes
every stream, including streams not explicitly closed by the scenario. Withholding a response can also target a named stream:
`* research runner: withhold the next TopicService.StreamWrite "A" WriteResponse`.

Each `WriteRequest` step sends exactly one gRPC message. All table rows become its `messages` list, in table order:

```gherkin
* TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
  | data    | seq_no |
  | first   | 1      |
  | second  | 2      |
```

`txId` is a client-side alias resolved to the real `WriteRequest.tx.id` and `WriteRequest.tx.session`. It applies to the whole
request; messages from different transactions require separate steps. For non-transactional writes, omit the parameters
(`WriteRequest messages:`) or use `WriteRequest{} messages:`. The codec is `CODEC_RAW`.

The table accepts only message fields `data` and `seq_no`. Omitted or empty `seq_no` becomes proto3 zero and is printed as
`seq_no=0`. Empty payloads are allowed. The entire table is validated before sending, so an invalid row cannot result in a
partially sent batch. The old `tx` column is rejected rather than silently splitting requests or ignoring a transaction.

Send and receive still run in separate goroutines; the step implicitly observes responses after sending its batch. Use
separate single-row steps when the experiment requires separate WriteRequests. Add a Go step only when an experiment
introduces a protocol action that the shared vocabulary cannot express.
