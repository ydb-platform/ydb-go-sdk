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

Command for measure performance:
```bash
redis-benchmark -p 6379 -q -c 10 -n 10000 -r 1000 -t get,set
```

- origin Redis server (v8.6.2):
```
SET: 32786.88 requests per second, p50=0.287 msec
GET: 31645.57 requests per second, p50=0.295 msec
```
- using KV API:
```
SET: 4194.63 requests per second, p50=2.175 msec
GET: 11111.11 requests per second, p50=0.847 msec
```
- using Query API
```
SET: 2649.71 requests per second, p50=3.439 msec
GET: 3895.60 requests per second, p50=1.991 msec
```

Summary table:

| CMD                 | Redis   | KV API  | `KV` / `Redis` | Query API | `Query` / `Redis` |
|---------------------|---------|---------|-----------------|-----------|--------------------|
| `SET`<br/>(p50, ms) | `0,287` | `2.175` | `x7.58`         | `3.439`   | `x11.98`           |
| `GET`<br/>(p50, ms) | `0.295` | `0.847` | `x2.87`         | `1.991`   | `x6.75`            |
