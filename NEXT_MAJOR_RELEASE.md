# Breaking changes for the next major release
- [x] Delete deprecated api package
- [x] Delete deprecated internalapi package
- [x] Delete deprecated parameter KeepAliveBatchSize from session pool
- [x] Delete deprecated ConnUsePolicy, EndpointInfo, WithEndpointInfo, WithEndpointInfoAndPolicy, ContextConn
- [x] Delete deprecated client option DefaultMaxQueryCacheSize, MaxQueryCacheSize and client query cache
- [x] Change `proto` codegen code in `api` from `internal/cmd/protoc-gen` tool to standard `protoc-gen-go` tool. 
  This need for change imports to standard. Current imports are deprecated and linters alarms
- [x] Replace grpc and protobuf libraries to actual
- [x] Replace all internal usages of `driver.Call()` and `driver.StreamRead()` to code-generated grpc-clients,
      which will be use driver as `grpc.ClientConnInterface`  provider.
- [ ] Delete deprecated Driver interface
- [ ] Remove or hide (do private) deprecated API for new `scanner`.
- [ ] Delete deprecated ready statistics from session pool
- [ ] Hide (do private) entity `table.Client` or `table.SessionPool` because it most difficultly for SDK users
- [ ] Extract auth package to neighbour project(-s) for isolation ydb-go-sdk from unnecessary dependencies
- [x] Extract coordination package to neighbour project as plugin over ydb-go-sdk
- [x] Extract ratelimiter package to neighbour project as plugin over ydb-go-sdk
- [ ] Extract experimental package to neighbour project as plugin over ydb-go-sdk
- [ ] Refactoring Trace API: exclude duplicates of data from closure Trace functions
- [x] Move traceutil from internal to root
