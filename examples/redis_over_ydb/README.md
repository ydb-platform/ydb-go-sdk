# Redis-like KV over YDB

This example is a small **key-value server with redis-like API** so you can use `redis-cli` / `redis-benchmark` for basic `GET`, `SET`, `SETEX`, `DEL`, `KEYS`, `PING`, and related stubs.

## Table path (database-relative)

This example keeps the configured table name **database-relative** by default (for example, `` `kv` ``).

In code, this repository uses both database-relative names and absolute paths built with `path.Join(db.Name(), tableRel)` in YQL/Query API examples. To stay consistent with the rest of the repo and with the **table service** (`BulkUpsert` / `ReadRows`), this example treats `YDB_KV_TABLE_PATH` / `WithTable` / `-table` as the table path relative to the current database and constructs the absolute path with `path.Join(db.Name(), tableRel)` where needed.

Default table: `kv`. Override with `sugar.NewKV(...).WithTable(...)` or `YDB_KV_TABLE_PATH`.

Create parent directories first (`ydb scheme mkdir`, or [`sugar.MakeRecursive`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/sugar#MakeRecursive)) before the first run if you use `WithCreateTableIfNotExists(...)`.

## Environment variables

| Variable            | Type   | Possible values | Default | Effect                                          |
|---------------------|--------|-----------------|---------|-------------------------------------------------|
| `YDB_KV_TABLE_PATH` | String |                 | `kv`    | Table path relative to<br/>the root of database |
| `YDB_API`           | Enum   | `query` / `kv`  | `kv`    | YDB API for work in<br/>GET/SET commands        |
| `YDB_KV_TABLE_LRU`  | Number | `>=0`           | 0       | LRU cache limit for table                       |

## Run

From the repository `examples/` directory:

```bash
export YDB_CONNECTION_STRING='grpc://localhost:2136/local' go run ./redis_over_ydb -addr :6379
```

Stop with **Ctrl+C** (SIGINT) or SIGTERM.

```bash
redis-cli -p 6379 PING
```

## Performance

Measured on [local-ydb](https://hub.docker.com/r/ydbplatform/local-ydb).
All performance metrics bellow are provided as an example.
Tuned production YDB installations produce different results.

Command for measure performance (concurrency=900):
```bash
redis-benchmark -p 6379 -q -c 900 -n 10000 -r 1000 -t get,set
```

- origin Redis server (v8.6.2):
```
SET: 62111.80 requests per second, p50=11.951 msec      
GET: 57803.47 requests per second, p50=13.335 msec
```
- redis-over-ydb using KV API (without session pool limitation):
```
SET: 17825.31 requests per second, p50=44.575 msec                    
GET: 10235.42 requests per second, p50=82.431 msec 
```
- redis-over-ydb using Query API (default session pool limit - 50)
```
SET: 5138.75 requests per second, p50=156.543 msec                    
GET: 1434.51 requests per second, p50=548.863 msec
```

### Summary table of latencies (p50, msec)

| CMD   | Redis    | KV API   | `KV` / `Redis` | Query API  | `Query` / `Redis` |
|-------|----------|----------|----------------|------------|-------------------|
| `SET` | `11.951` | `44.575` | `x3.73`        | `156.543`  | `x13.09`          |
| `GET` | `13.335` | `82.431` | `x6.18`        | `548.863`  | `x41.16`          |

### Summary table of throughput (requests per second)

| CMD   | Redis   | KV API  | `Redis` / `KV` | Query API | `Redis` / `Query` |
|-------|---------|---------|----------------|-----------|-------------------|
| `SET` | `62111` | `17825` | `x3.48`        | `5138`    | `x12.09`          |
| `GET` | `57803` | `10235` | `x5.65`        | `1434`    | `x40.31`          |
