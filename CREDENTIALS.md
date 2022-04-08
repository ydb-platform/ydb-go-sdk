## Credentials <a name="Credentials"></a>

Driver contains two options for making simple `credentials.Credentials`:
- `ydb.WithAnonymousCredentials()`
- `ydb.WithAccessTokenCredentials("token")`

Another variants to get `credentials.Credentials` object provides with external packages:

Package | Type | Description                                                                                                                                                                                 | Link of example usage
--- | --- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ---
[ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc) | credentials | credentials provider for Yandex.Cloud | [yc.WithServiceAccountKeyFileCredentials](https://github.com/ydb-platform/ydb-go-yc/blob/master/internal/cmd/connect/main.go#L22) [yc.WithInternalCA](https://github.com/ydb-platform/ydb-go-yc/blob/master/internal/cmd/connect/main.go#L22) [yc.WithMetadataCredentials](https://github.com/ydb-platform/ydb-go-yc/blob/master/internal/cmd/connect/main.go#L24)
[ydb-go-yc-metadata](https://github.com/ydb-platform/ydb-go-yc-metadata) | credentials | metadata credentials provider for Yandex.Cloud | [yc.WithInternalCA](https://github.com/ydb-platform/ydb-go-yc-metadata/blob/master/options.go#L23) [yc.WithCredentials](https://github.com/ydb-platform/ydb-go-yc-metadata/blob/master/options.go#L17)
[ydb-go-sdk-auth-environ](https://github.com/ydb-platform/ydb-go-sdk-auth-environ) | credentials | create credentials from environ | [ydbEnviron. WithEnvironCredentials](https://github.com/ydb-platform/ydb-go-sdk-auth-environ/blob/master/env.go#L11)
