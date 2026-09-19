# Transactional StreamWrite server contracts

This is the first implementation step from the [StreamWrite pool design](https://wiki.yandex-team.ru/users/zkpo/pul-streamwrite-sessijj-dlja-tranzakcionnogo-write/).
These executable expectations specify the server behavior on which the proposed SDK session and pool depend.
They do not implement the pool or select a public SDK API.

Unlike the adjacent `research/` experiments, every scenario asserts its expected outcome. A mismatch fails the Go test.
The interactive runner discovers this directory automatically and reports `PASS` / `FAIL` for its files; research keeps
`RECORDED` / `ERROR`. An infrastructure error also makes a contract run unsuccessful; inspect the error before attributing
that failure to the server. Run all files in this directory when checking the initial contract set.

## Contracts in this first set

| Contract | Executable coverage | Why the SDK needs it |
| --- | --- | --- |
| C1. Producerless sessions do not deduplicate repeated sequence numbers across requests. | [producerless_delivery.feature](producerless_delivery.feature): overlapping batches in one transaction; identical ranges in two open transactions. | A new stream-local automatic counter needs no durable producer state. All accepted payloads must survive commit. |
| C2. ACKs identify acknowledged sequence numbers and their results. | [producerless_delivery.feature](producerless_delivery.feature), [shared_stream_lifecycle.feature](shared_stream_lifecycle.feature), [producer_deduplication.feature](producer_deduplication.feature): requests of different sizes, overlaps, identical ranges, interleaved transactions and `skipped`. | The session uses seqNo as the sole ACK correlation key. |
| C3. Sequence numbers are positive and strictly increasing inside each request. An invalid batch is rejected with BAD_REQUEST and no ACKs. | [request_sequence_validation.feature](request_sequence_validation.feature): zero, negative, repeated and decreasing numbers, with and without a producer. | Manual-mode batching must split at non-increasing numbers while retaining all original messages. |
| C4. A healthy stream accepts multiple transactions; commit/rollback affects only that transaction, and the stream remains usable. | [shared_stream_lifecycle.feature](shared_stream_lifecycle.feature): interleaved A/B, commit B while A is open, rollback A, then commit C on the same stream. Producer conflict recovery also writes C on the existing stream. | Transaction completion must release its own state without closing a shared healthy session. |
| C5. Producer deduplication preserves original contents; `written_in_tx` is not commit success. | [producer_deduplication.feature](producer_deduplication.feature): overlap returns `skipped`; both conflicting transactions receive ACKs, then the second commit is `ABORTED`. | The SDK must forward server results and must not infer a successful attempt from ACKs or a sequence conflict. |
| C6. A rolled-back user sequence number remains writable through a new producer session. | [producer_deduplication.feature](producer_deduplication.feature): commit 7, rollback 8, reopen, then commit 8 and read both committed payloads. | Full transaction replay can preserve user sequence numbers after confirmed rollback. |
| C7. One transaction can write through multiple partition streams, with independent sequence spaces. | [multi_partition_transaction.feature](multi_partition_transaction.feature): two partitions without producers and with distinct producers; complete readback after commit. | A multiwriter transaction must publish all accepted messages, not just the successful partition. |
| C8. Lost ACK followed by confirmed rollback permits replay of the whole transaction with every payload delivered once. | [lost_ack_rollback.feature](lost_ack_rollback.feature): real ACK is withheld, old streams close, rollback succeeds, both partitions are replayed through new streams. | Retry belongs to the whole transaction; session-level resend is unnecessary. |
| C9. A split can invalidate commit even after `written_in_tx`; replay on active children succeeds after that known rejection. | [split_before_commit.feature](split_before_commit.feature): actual inactive parent/active children, ACK after split, aborted commit, complete replay/readback. | Successful Flush cannot hide Commit failure; the next attempt needs refreshed routing. |
| C10. The next ordinary write on a session bound to a split parent fails with OVERLOADED and the stream ends. | [split_session_and_discovery.feature](split_session_and_discovery.feature): successful pre-split write, split, rejected post-split write and EOF. | A failed session must be replaced; OVERLOADED alone does not establish that fresh routing metadata is available. |
| C11. DescribeTopic eventually exposes the split children after writer rejection; earlier responses may still show the old topology. | [split_session_and_discovery.feature](split_session_and_discovery.feature): ordinary write probes during split, timestamped descriptions, inactive parent and active children in a request started after rejection. | Route refresh must tolerate a description that still lacks the children. |
| C12. DescribeTopic reports the current activity of every partition. | [partition_activity.feature](partition_activity.feature): the initial partition is active; after split the parent is inactive and both children are active. | Partition Source can use `active` as the server-authoritative writable state. |

The set has **21 scenarios in 9 feature files**, including the eight sequence-validation examples.

`Then contract:` steps inspect cloned protobuf responses and the recorded transaction RPC result, not text in the trace.
Each ACK table row is one `(seq_no, result)` pair. The table describes all ACKs expected so far on that physical stream.
ACKs are correlated only by seqNo. For each seqNo, assertions check the expected ACK count and result values.
Neither ACK order within/between WriteResponses nor response batching is asserted. Checks include Init success, missing/extra ACKs and wrong sequence
numbers/results. ACK assertions wait for the expected number of message ACKs, not one response per WriteRequest.

`* client: send TopicService.StreamWrite requests without waiting for ACKs` changes how subsequent write steps execute:
each step sends its request and proceeds without waiting for that request's ACK. Requests are still sent sequentially on
each stream; responses arrive independently. This permits several requests to be in flight. The later
`Then contract: ... has exactly these ACKs` matches the received ACKs by seqNo without relying on their arrival order.

`* client: withhold the next TopicService.StreamWrite "OldRight" WriteResponse` injects the lost-ACK condition: the test client
records the real response, withholds it from the scenario and cancels that stream. Both controls reuse the shared transport
helpers; all expected server outcomes are asserted by the contract steps.

InitResponse field values and WriteResponse.partition_id are not assertions. The scenarios do not request LastSeqNo.
ACK seqNo checks match the numbers actually sent and verify response correlation.

The partition-activity contract compares the complete `(partition_id, active)` mapping returned by DescribeTopic before
and after a split. `active` is a proto3 scalar, so its wire presence is not observable after decoding; the contract asserts
the decoded value for every returned partition and rejects missing or extra partitions.

General scenarios omit partition_id in StreamWrite Init and subscribe to the topic without a partition filter when reading.
Their single-partition fixture provides an ordered readback of `(seq_no, data)` without asserting a partition ID.
Explicit partition IDs remain in the multiple-partition and split scenarios, where routing is part of the tested behavior.
Their `(partition_id, seq_no, data)` tables preserve message order within each partition and disregard arrival order between
partitions. Offsets and intermediate visibility are not checked; delivery assertions read the final committed messages.

Repeated seqNo values in the producerless scenarios exercise server deduplication behavior. These contracts check the ACK
count and result values for each seqNo, along with the final payloads.

## Split observations and expectations

The separate [research feature](../research/split_session_and_discovery.feature) observes idle-stream behavior and writes
while sampling DescribeTopic during a real AlterTopic split with PAUSED auto partitioning. Probes are ordinary writes,
numbered from 1, with one outstanding request at a time. Probes stop on writer rejection or after one final probe following
a successful AlterTopic response. A fresh post-Alter description completes the research even without writer rejection;
contracts assert rejection and eventual children separately. The sampling interval is a minimum cadence, not a bound on
RPC duration. The client records each request's start and completion time and joins
the concurrent AlterTopic RPC before finishing the step. These are raw gRPC calls with no SDK retries or metadata cache.

On `main.db11cbd`, one of three runs observed OVERLOADED at +15.901ms, then a DescribeTopic started at +15.967ms still
returned only the active parent at +18.328ms. The next request returned the children at +20.720ms. Synchronous AlterTopic
completed at +18.539ms. Two other runs already saw children in the first description after writer rejection.

The discovery contract accepts both immediate and delayed child visibility and samples further if needed, bounded by the
scenario context. It requires a description requested after rejection; it does not assert a minimum propagation delay.
A mandatory stale response in every run would depend on scheduling. The separate termination contract checks an ordinary
write after split. No unsolicited error arrived during the research's 1000ms idle window. Transactional writes already
staged before split retain the distinct ACK/Commit behavior covered by C9.

## Reproduce

From `tests/server_contracts`, using an already running YDB:

```bash
YDB_CONNECTION_STRING=grpc://localhost:2136/local \
YDB_SERVER_FEATURE_PATH=topic/contracts \
go test -race -v -count=1 -timeout=8m -run '^TestServerFeatures$' ./internal/research
```

Or use the existing Docker runner to select one feature; it prints the server version and immutable image identity:

```bash
go run ./cmd/server-contracts -version trunk \
  -test topic/contracts/producerless_delivery.feature
```

The runner starts Compose with `--pull always`, so mutable tags such as `latest`, `edge` and `trunk` are resolved from the
registry before every run.

The suite uses isolated temporary topics and serializable read-write Query transactions. It never calls the SDK's
transactional topic writer or retries a rejected transaction implicitly. Each replay is explicit in its scenario.

For local assertion and runner checks:

```bash
go test -race -skip '^TestServerFeatures$' ./...
go vet ./...
golangci-lint run ./...
```

The ACK checker accepts arbitrary InitResponse fields and WriteResponse.partition_id. Unit tests accept merged, split and
reordered ACKs, and reject missing/extra ACKs, wrong sequence numbers, repeated-pair counts, results and server rejection.
A controlled stream test sends two requests containing three messages and returns their ACKs out of order, in one combined
response or three separate responses. The ACK wait finishes while the stream remains open. An empty observation cannot pass.

## Validation on 2026-09-11

- Checkout: `server-topic-research`, HEAD `cea553fdf1c8630ca6f3eb0029a9c0cdb8da13c2`, with these uncommitted changes.
- YDB: `main.db11cbd`, read from the running server's Viewer API.
- Docker image ID: `sha256:71917a5c5d7f23ce956355d191ce1442b6b9a31e951606648feee57546c9da32`.
- All 18 scenarios passed with `-race` in 50.15 seconds using flat ACK expectations and waiting by message ACK count.
- Nested-module unit tests with `-race`, `go vet`, and `golangci-lint` passed. SDK-root `go test -race -timeout=5m ./...`
  and `golangci-lint run ./...` also passed on the working checkout.

These results validate this server build, not an untested compatibility range. Partition-change research and benchmark
baseline measurements from later parts of the design are not included in this run.

## Validation on 2026-09-14

All 18 scenarios passed against local YDB with `-race` in 50.21 seconds with ACK matching by seqNo and checks of the
corresponding result values and counts, independent of ACK order and response batching. General scenarios omit partition
selection, and offsets are not asserted.
Nested-module unit tests with `-race`, `go vet`, and `golangci-lint` passed. Reordered-ACK tests failed on the previous
order-sensitive comparator and passed after the change.
The SDK-root checks listed above belong to the earlier validation; this update changes only the server-contract module.

After adding C10 and C11, all **20 contract scenarios passed with `-race` in 52.48 seconds** on the same local server.
The two separate research scenarios completed three diagnostic runs and a final run with `-race`; the diagnostic runs
established the timing observations above. Nested-module unit tests with `-race`, `go vet`, and `golangci-lint` passed.

## Validation on 2026-09-19

The partition-activity contract was run against three Docker image tags:

- `latest`: YDB `26.2.1.14.1bd0f9c`, image digest
  `sha256:9e46fd45875551a75bcf34d0bb9ca0baa1d8763a4ccf2070af45f4467c4b7402` — **FAIL**. The initial description
  reported `map[0:true]`; AlterTopic returned `SUCCESS`, but the topology remained `map[0:true]` for the full scenario
  timeout, so this build did not reach the post-split assertion.
- `edge`: YDB `26.3.1.15.2c6b0d7`, image digest
  `sha256:afbed0470c7e599e40d4caa99d7418b18fdff002621926aa465716af29f5e6c9` — **PASS**. DescribeTopic changed from
  `map[0:true]` to `map[0:false 1:true 2:true]` after the split.
- `trunk`: YDB `main.f5322db`, image digest
  `sha256:ca76a10ab5ef8d2ef3b923375f95d486e86f6ce9dedbac1c3b9502d266f4b2c2` — **PASS**. DescribeTopic changed from
  `map[0:true]` to `map[0:false 1:true 2:true]` after the split.

The failed `latest` run establishes the initial `active=true` value but is inconclusive about the post-split value, because
that build did not perform the requested split. Each run pulled its tag from the registry first. Nested-module tests
excluding live server scenarios passed with `-race`.

## Remaining contracts before the corresponding implementation stage

These are requirements from the design, not claims established by this set:

- **Producer admission and pooled WaitInitInfo:** decide which transactions may share one producer and define how to obtain
  a current `LastSeqNum` on an already initialized session. The server conflict/replay tests above do not decide those SDK
  policies or assert LastSeqNo.
- **Additional transaction participants:** multiple streams into the same partition, multiple topics and the application
  workload `UPSERT + TopicWrite + Commit`; verify atomic visibility and per-stream order without inventing cross-stream order.
- **Partition Source:** finish research for manual changes under DISABLED, one full range during enablement, multiple split
  generations, merge, and complete issue trees at Init/Write/Commit. Do not infer split from every ABORTED or hard-code
  issue 2011 as the TLI classifier. The split fixture above asserts the actual topology, not just Alter success.
- **Direct routing and authorization:** generation/node changes, serialized UpdateToken and terminal authorization failure
  with active transactions. These require their dedicated routing/authentication fixtures.
- **Ambiguous Commit:** determine the actual Query retryer behavior and safe application action. The known-rollback replay
  test provides no guarantee of duplicate-free replay after a lost Commit result.
- **SDK component behavior:** implementation selection/options, whole-Write validation, queue ownership, encoding order,
  sequence assignment, late ACK/cancellation, Flush/Commit callbacks, pool lifetime, lazy initialization, shared snapshots
  and route refresh. Add those unit/integration tests with their respective implementation; this directory checks the server.

Research evidence motivating this set is in the adjacent features for overlapping batches, concurrent Query transactions,
producer transaction state, lost ACKs and split replay. Their dated observations are preserved unchanged.
