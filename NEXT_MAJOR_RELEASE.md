# Breaking changes for the next major release
- [ ] Delete deprecated ConnUsePolicy, EndpointInfo, WithEndpointInfo, WithEndpointInfoAndPolicy, ContextConn
- [ ] Delete deprecated Driver interface
- [ ] Delete deprecated api package
- [ ] Delete deprecated internalapi package
- [ ] Delete deprecated parameter KeepAliveBatchSize from session pool
- [ ] New `scanner`, which returns High-level types sych as Interval, Datetime, String and other. High-level type must
  provide low-level types such as string, time.Duration, etc. This change need to exclude custom transformations
  and interpretation low-level types as high-level entity
