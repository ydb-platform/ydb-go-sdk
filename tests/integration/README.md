# Package `integration`

Package `integration` contains only integration tests for `ydb-go-sdk`. All test files must have build tag
```go
//go:build integration
// +build integration
```
for run this test files as integration tests int github action `integration`.

StrictSerializableRW tests require a YDB nightly server with
`TableServiceConfig.EnableStrictSerializableIsolation` enabled. Run them locally
with `bash .github/scripts/strict-serializable-integration.sh` from the repository
root. The script starts and removes its own Docker containers. Set
`YDB_STRICT_TEST_PORT` if port 2136 is already in use.
