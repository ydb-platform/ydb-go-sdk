# Migration from `ydb-go-sdk/v2` to `ydb-go-sdk/v3` with `database/sql` API driver usages

> Article contains some cases for migrate from `github.com/yandex-cloud/ydb-go-sdk/v2/ydbsql` to `github.com/ydb-platform/ydb-go-sdk/v3`

> Note: the article is being updated.

## Read-only isolation levels

In `ydb-go-sdk/v2/ydbsql` was allowed `sql.LevelReadCommitted` and `sql.LevelReadUncommitted` isolation levels for read-only interactive transactions. It implements with fake transaction with true `OnlineReadOnly` transaction control on each query inside transaction.

`ydb-go-sdk/v3` allowed only `sql.LevelSnapshot` for read-only interactive transactions. Currently, snapshot isolation implements over fake transaction with true `OnlineReadOnly` transaction control.
YDB implements snapshot isolation, but this feature is not deployed on YDB clusters now. After full deploying on each YDB cluster fake transaction will be replaced to true read-only interactive transaction with snapshot isolation level.  