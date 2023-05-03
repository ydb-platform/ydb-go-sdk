# Package `integration`

Package `integration` contains only integration tests for `ydb-go-sdk`. All test files must have build tag
```go
//go:build integration
// +build integration
```
for run this test files as integration tests int github action `integration`.