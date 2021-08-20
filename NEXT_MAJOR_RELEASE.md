# Breaking changes for the next major release
- [ ] Delete deprecated api package
- [ ] Delete deprecated internalapi package
- [ ] Delete deprecated parameter KeepAliveBatchSize from session pool
- [ ] Delete deprecated client option DefaultMaxQueryCacheSize, MaxQueryCacheSize and client query cache
- [ ] Change `proto` codegen from `internal/cmd/protoc-gen` to standard `protoc-gen-go`. This need for change
  imports to standard. Current imports are deprecated and linters alarms
- [ ] New `scanner`, which returns High-level types sych as Interval, Datetime, String and other. High-level type must
  provide low-level types such as string, time.Duration, etc. This change need to exclude custom transformations
  and interpretation low-level types as high-level entity
- [ ] Hide (do private) entity `table.Client` or `table.SessionPool` because it most difficultly for SDK users
