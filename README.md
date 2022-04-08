# ydb-go-sdk

[![PkgGoDev](https://pkg.go.dev/badge/github.com/ydb-platform/ydb-go-sdk/v3)](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3)
[![GoDoc](https://godoc.org/github.com/ydb-platform/ydb-go-sdk/v3?status.svg)](https://godoc.org/github.com/ydb-platform/ydb-go-sdk/v3)
![tests](https://github.com/ydb-platform/ydb-go-sdk/workflows/tests/badge.svg?branch=master)
![lint](https://github.com/ydb-platform/ydb-go-sdk/workflows/lint/badge.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ydb-platform/ydb-go-sdk/v3)](https://goreportcard.com/report/github.com/ydb-platform/ydb-go-sdk/v3)
[![codecov](https://codecov.io/gh/ydb-platform/ydb-go-sdk/branch/master/graph/badge.svg?precision=2)](https://app.codecov.io/gh/ydb-platform/ydb-go-sdk)

[YDB](https://github.com/ydb-platform/ydb) native Go's driver.

Supports `table`, `discovery`, `coordination`, `ratelimiter`, `scheme` and `scripting` clients for `YDB`.

Simple example see in [example](example_table_test.go). More examples of usage placed in [examples](https://github.com/ydb-platform/ydb-go-examples) repository.

See also [CREDENTIALS.md](CREDENTIALS.md) about supported YDB credentials.

See [DEBUG.md](DEBUG.md) about supported debug tooling over `ydb-go-sdk`.

See [ENVIRON.md](ENVIRON.md) about supported environment variables of `ydb-go-sdk`.
