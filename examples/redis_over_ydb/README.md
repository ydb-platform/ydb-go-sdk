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

## Run

From the repository `examples/` directory:

```bash
export YDB_CONNECTION_STRING='grpc://localhost:2136/local'
go run ./redis_over_ydb -addr :6379
```

Stop with **Ctrl+C** (SIGINT) or SIGTERM.

```bash
redis-cli -p 6379 PING
```

## Performance

Command for measure performance (concurency=900):
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
SET: 16339.87 requests per second, p50=46.495 msec                    
GET: 21786.49 requests per second, p50=35.455 msec 
```
- redis-over-ydb using Query API (default session pool limit - 50)
```
SET: 5144.03 requests per second, p50=151.551 msec                    
GET: 4833.25 requests per second, p50=137.727 msec 
```

Summary table of latencies (p50, msec):

| CMD   | Redis    | KV API   | `KV` / `Redis` | Query API  | `Query` / `Redis` |
|-------|----------|----------|----------------|------------|-------------------|
| `SET` | `11.951` | `46.495` | `x3.89`        | `151.551`  | `x12.68`          |
| `GET` | `13.335` | `35.455` | `x2.66`        | `137.727`  | `x10.33`          |

Summary table of throughput (requests per second):

| CMD   | Redis   | KV API  | `Redis` / `KV` | Query API | `Redis` / `Query` |
|-------|---------|---------|----------------|-----------|-------------------|
| `SET` | `62111` | `16339` | `x3.80`        | `5144`    | `x12.07`          |
| `GET` | `57803` | `21786` | `x2.65`        | `4833`    | `x12.00`          |
