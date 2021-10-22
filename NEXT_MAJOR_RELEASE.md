# Breaking changes for the next major release
- [ ] Delete deprecated ready statistics from session pool
- [ ] Delete deprecated api package
- [ ] Delete deprecated internalapi package
- [ ] Delete deprecated parameter KeepAliveBatchSize from session pool
- [ ] Delete deprecated client option DefaultMaxQueryCacheSize, MaxQueryCacheSize and client query cache
- [ ] Change `proto` codegen from `internal/cmd/protoc-gen` to standard `protoc-gen-go`. This need for change
  imports to standard. Current imports are deprecated and linters alarms
- [ ] Remove and hide deprecated API for new `scanner`.
- [ ] Hide (do private) entity `table.Client` or `table.SessionPool` because it most difficultly for SDK users
- [ ] Drop `table.SessionProvider.PutBusy()` interface func
- [ ] Drop `Retry.MustCheckSession()` func
- [ ] Drop `RetryCheckSession` constant
- [ ] Drop `table.SessionPoolStats.BusyCheck` counter
- [ ] Drop `ydbsql.WithSessionPoolBusyCheckInterval()`
- [ ] Drop `connect.WithSessionPoolBusyCheckInterval()`
- [ ] Drop marked as deprecated some retry constants
- [ ] Change `Backoff` interface
- [ ] Refactoring of ydbsql connector for split driver runtimes on Connect stage
