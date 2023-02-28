# ydb-go-examples

> examples for work with YDB 

## Navigation of examples

| Example                            | Description                                                     | Run command                                                                                                          |
|------------------------------------|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| `auth/access_token_credentials`    | authenticate with access token credentials                      | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/access_token_credentials#readme)    |
| `auth/anonymous_credentials`       | authenticate with anonymous credentials                         | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/anonymous_credentials#readme)       |
| `auth/metadata_credentials`        | authenticate with metadata credentials                          | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/metadata_credentials#readme)        |
| `auth/service_account_credentials` | authenticate with service account credentials                   | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/service_account_credentials#readme) |
| `auth/environ`                     | authenticate using environment variables                        | see [README.md](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/environ#readme)                     |
| `basic/native`                     | store and read the series with native driver                    | `make basic`                                                                                                         |
| `basic/database_sql`               | store and read the series with database/sql driver              | `make database_sql`                                                                                                  |
| `serverless/healthcheck`           | healthcheck site by URL (yandex function and local http-server) | `make healthcheck`                                                                                                   |
| `serverless/url_shortener`         | URL shortener example (yandex function and local http-server)   | `make url_shortener`                                                                                                 |
| `bulk_upsert`                      | bulk upserting data                                             | `make bulk_upsert`                                                                                                   |
| `containers`                       | containers example                                              | `make containers`                                                                                                    |
| `ddl`                              | DDL requests example                                            | `make ddl`                                                                                                           |
| `decimal`                          | decimal store and read                                          | `make decimal`                                                                                                       |
| `pagination`                       | pagination example                                              | `make pagination`                                                                                                    |
| `partitioning_policies`            | partitioning_policies example                                   | `make partitioning_policies`                                                                                         |
| `read_table`                       | read table example                                              | `make read_table`                                                                                                    |
| `topic/cdc-cache-bus-freeseats`    | example of use cdc for cache updates in web application         | `go run topic/cdc-example-cache-freeseats/*.go`                                                                      |
| `topic/cdc-fill-and-read`          | change table records and read cdc stream                        | `go run topic/cdc/*.go`                                                                                              |
| `ttl`                              | TTL using example                                               | `make ttl`                                                                                                           |
| `ttl_readtable`                    | TTL using example                                               | `make ttl_readtable`                                                                                                 |

Run command needs prepared environ like this:
```bash
export YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=~/.ydb/SA.json
export YDB_CONNECTION_STRING="grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etn02qhd0tfkrq4riqgd"
```
