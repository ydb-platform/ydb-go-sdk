# Package `integration`

Package `integration` contains only integration tests for `ydb-go-sdk`. All test files must have build tag
```go
//go:build integration
// +build integration
```
for run this test files as integration tests int github action `integration`.

Arrow tests and benchmarks live in a [separate Go module](arrow/README.md) so
Apache Arrow Go is not a dependency of the SDK module. Run them from
`tests/integration/arrow`; `go test -tags integration ./tests/integration`
does not include nested modules.

The `edge` entries in the integration matrix run the StrictSerializableRW tests.
To run them locally, point `YDB_CONNECTION_STRING` at a YDB server with
`TableServiceConfig.EnableStrictSerializableIsolation` enabled and run:

```sh
YDB_STRICT_SERIALIZABLE_ENABLED=1 go test -race -tags integration ./tests/integration -run '^TestQueryStrictSerializable'
```
