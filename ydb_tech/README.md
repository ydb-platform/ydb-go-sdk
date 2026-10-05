# Executable documentation examples

`ydb_tech` contains the executable sources used by ydb.tech documentation.
The `Examples (ydb_tech)` CI job builds and runs every registered example against
this checkout and a local YDB instance.

Run all examples from the repository root:

```sh
bash ydb_tech/run.sh
```

Each example has its own directory and a `run.sh` entry point. The common runner
finds these entry points automatically and fails if any example fails or times out.
It requires Python 3 for portable process timeouts. Set `YDB_TECH_TIMEOUT_SECONDS`
to change the default 180-second limit per example.

To add an example:

1. Add a directory with executable source and a `run.sh` entry point.
2. Register its project in the SDK build if the language requires it.
3. Mark documentation regions with `[BEGIN name]` and `[END name]` comments.
4. Keep each region nonempty, unique within its file, and free of nested regions.

No workflow changes are needed for a new example. The topic example is the first
scenario; its README describes SDK-specific build and connection settings.
