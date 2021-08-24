# Breaking changes for the next major release
- [x] Delete deprecated api package
- [x] Delete deprecated internalapi package
- [x] Delete deprecated parameter KeepAliveBatchSize from session pool
- [x] Delete deprecated ConnUsePolicy, EndpointInfo, WithEndpointInfo, WithEndpointInfoAndPolicy, ContextConn
- [x] Delete deprecated client option DefaultMaxQueryCacheSize, MaxQueryCacheSize and client query cache
- [x] Replace grpc and protobuf libraries to actual
- [x] Replace all internal usages of `driver.Call()` and `driver.StreamRead()` to code-generated grpc-clients,
      which will be use driver as `grpc.ClientConnInterface`  provider.
- [ ] Delete deprecated Driver interface
- [ ] Remove or hide (do private) deprecated API for new `scanner`.
- [ ] Hide (do private) entity `table.Client` or `table.SessionPool` because it most difficultly for SDK users
