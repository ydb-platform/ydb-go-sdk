# Topic examples for ydb.tech

This executable supplies the Go snippets in the topic reference on ydb.tech.
It uses the SDK from the root module and verifies management, payloads, codecs,
metadata, commits, transactional reads and writes, and autoscaling settings.

From the repository root, with a local YDB instance:

```sh
go run ./examples/ydb_tech/topic
```

`YDB_CONNECTION_STRING` defaults to `grpc://localhost:2136/local`.
Topics have unique names and are removed on completion. The run has a two-minute deadline.
The client offset scenario uses an in-memory store; replace it with persistent storage
when offsets must survive a process restart.
