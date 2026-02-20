# Package `integration`

Package `integration` contains only integration tests for `ydb-go-sdk`. All test files must have build tag
```go
//go:build integration
// +build integration
```
for run this test files as integration tests int github action `integration`.

## Known issues

Some tests are skipped under certain YDB versions due to upstream or environment limitations. See [Known Issues](../../docs/KNOWN_ISSUES.md) for details.