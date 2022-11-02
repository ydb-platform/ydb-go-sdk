## Instructions for run test and linter checks locally

### Prerequisites

- Docker. See [official documentations](https://docs.docker.com/engine/install/) for install `docker` to your operating system 
- go >= 1.18. See [official instructions](https://go.dev/doc/install) for install `golang` to your operating system
- golangci-lint >= 1.48.0. See [official instructions](https://golangci-lint.run/usage/install/) for install `golangci-lint` to your operating system

### Run linter checks

```sh
$ golangci-lint run ...
```

### Run tests

### Only unit tests

```sh
$ go test -race -tags fast ./... 
```

### All tests (include integration tests)

```sh
$ docker run -itd --name ydb -dp 2135:2135 -dp 2136:2136 -dp 8765:8765 -v `pwd`/ydb_certs:/ydb_certs -e YDB_LOCAL_SURVIVE_RESTART=true -e YDB_USE_IN_MEMORY_PDISKS=true -h localhost cr.yandex/yc/yandex-docker-local-ydb:latest
$ YDB_CONNECTION_STRING="grpcs://localhost:2135/local" YDB_SSL_ROOT_CERTIFICATES_FILE="`pwd`/ydb_certs/ca.pem" YDB_SESSIONS_SHUTDOWN_URLS="http://localhost:8765/actors/kqp_proxy?force_shutdown=all" go test -race ./... 
$ docker stop ydb
```
