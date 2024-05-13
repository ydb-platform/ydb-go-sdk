# How to contribute

ydb-go-sdk (and YDB also) is an open project, and you can contribute to it in many ways. You can help with ideas, code, or documentation. We appreciate any efforts that help us to make the project better.

Thank you!

## Table of contents
  * [Legal Info](#legal-info)
  * [Technical Info](#technical-info)
    * [Instructions for checks code changes locally](#instructions-for-checks-code-changes-locally)
      + [Prerequisites](#prerequisites)
      + [Run linter checks](#run-linter-checks)
      + [Run tests](#run-tests)
        - [Only unit tests](#only-unit-tests)
        - [All tests (include integration tests)](#all-tests)

## Technical Info

1. Check for open issues or open a fresh issue to start a discussion around a feature idea or a bug.
2. Fork the repository <https://github.com/ydb-platform/ydb-go-sdk> on GitHub to start making your changes to the **master** branch (or branch off of it).
3. Write a test which shows that the bug was fixed or that the feature works as expected.
4. Send a pull request and bug the maintainer until it gets merged and published.

### Instructions for checks code changes locally

#### Prerequisites

- Docker. See [official documentations](https://docs.docker.com/engine/install/) for install `docker` to your operating system
- go >= 1.18. See [official instructions](https://go.dev/doc/install) for install `golang` to your operating system
- golangci-lint >= 1.48.0. See [official instructions](https://golangci-lint.run/usage/install/) for install `golangci-lint` to your operating system

#### Run linter checks

All commands must be called from project directory.

```sh
golangci-lint run ./...
```

#### Run tests

All commands must be called from project directory.

##### Only unit tests

```sh
go test -race -tags fast ./... 
```

##### All tests

```sh
docker run -itd --name ydb -dp 2135:2135 -dp 2136:2136 -dp 8765:8765 -v `pwd`/ydb_certs:/ydb_certs -e YDB_LOCAL_SURVIVE_RESTART=true -e YDB_USE_IN_MEMORY_PDISKS=true -h localhost ydbplatform/local-ydb:latest
export YDB_CONNECTION_STRING="grpcs://localhost:2135/local"
export YDB_SSL_ROOT_CERTIFICATES_FILE="`pwd`/ydb_certs/ca.pem"
export YDB_SESSIONS_SHUTDOWN_URLS="http://localhost:8765/actors/kqp_proxy?force_shutdown=all"
go test -race ./... 
docker stop ydb
```
