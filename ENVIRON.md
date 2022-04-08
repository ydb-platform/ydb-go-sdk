## Environment variables <a name="Environ"></a>

Name | Type | Default | Description
--- | --- | --- | ---
`YDB_SSL_ROOT_CERTIFICATES_FILE` | `string` | | path to certificates file
`YDB_LOG_SEVERITY_LEVEL` | `string` | `quiet` | severity logging level. Supported: `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`
`YDB_LOG_NO_COLOR` | `bool` | `false` | set any non empty value to disable colouring logs
`GRPC_GO_LOG_VERBOSITY_LEVEL` | `integer` | | set to `99` to see grpc logs
`GRPC_GO_LOG_SEVERITY_LEVEL` | `string` | | set to `info` to see grpc logs

