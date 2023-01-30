# Package tests

Package tests contains only integration tests for ydb-go-sdk. All test files must have build tag
```go
//go:build !fast
// +build !fast
```
for run this test files as integration tests int github action `tests`.