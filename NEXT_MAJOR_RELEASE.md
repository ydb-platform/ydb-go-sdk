# Breaking changes for the next major release
- [x] Delete deprecated api package
- [x] Delete deprecated internalapi package
- [x] Delete deprecated parameter KeepAliveBatchSize from session pool
- [x] New `scanner`, which returns High-level types sych as Interval, Datetime, String and other. High-level type must
  provide low-level types such as string, time.Duration, etc. This change need to exclude custom transformations
  and interpretation low-level types as high-level entity
- [x] Delete deprecated client option DefaultMaxQueryCacheSize, MaxQueryCacheSize and client query cache
- [x] Replace grpc and protobuf libraries to actual
- [ ] Delete deprecated Driver interface
- [x] Replace all usages of `driver.Call()` and `driver.StreamRead` to native grpc-clients 
- [ ] Remove or hide (do private) deprecated API for new `scanner`.
- [ ] Hide (do private) entity `table.Client` or `table.SessionPool` because it most difficultly for SDK users
