# Using transactions over YDB Query Service client

## Running the example
```bash
go run -ydb="grpcs://endpoint/?database=database"
```

## Strict serializable read-write transactions

Use `query.WithStrictSerializableReadWrite()` when beginning an explicit Query Service transaction, or `query.StrictSerializableReadWriteTxControl(query.CommitTx())` for a single committed query. The existing serializable mode remains the default.

An explicit transaction exposes the commit timestamp after `CommitTx` succeeds:

```go
var commitTimestamp *query.VirtualTimestamp
err := db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
	tx, err := session.Begin(ctx, query.TxSettings(query.WithStrictSerializableReadWrite()))
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := tx.Exec(ctx, "UPSERT INTO ..."); err != nil {
		return err
	}
	if err := tx.CommitTx(ctx); err != nil {
		return err
	}
	commitTimestamp = tx.(query.CommitTimestampProvider).CommitTimestamp()
	return nil
})
```

For an `ExecuteQuery` commit, use `query.WithCommitTimestamp` with `Exec`, `Query`, `QueryRow`, or `QueryResultSet`:

```go
err := db.Query().Exec(ctx, "UPSERT INTO ...",
	query.WithTxControl(query.StrictSerializableReadWriteTxControl(query.CommitTx())),
	query.WithCommitTimestamp(func(timestamp *query.VirtualTimestamp) {
		commitTimestamp = timestamp
	}),
)
```

The callback runs only after the final response part has been read. For streaming `Query` results, `query.CommitTimestampProvider` also exposes the value after the result is fully consumed or closed. `Client.Query` materializes the response and retains the timestamp on its returned result.

The server may omit the timestamp, including for read-only transactions or transactions without write effects. In that case `CommitTimestamp()` returns `nil` and the callback does not run. `PlanStep()` and `TxID()` return unsigned 64-bit values. `Compare` orders two timestamps by plan step, then transaction ID. It accepts only values issued through the same Query client with a known database name; values from separate connections cannot be compared, even when those connections use the same database path. The SDK does not have a server-provided database identity that could prove two separate connections refer to the same database.
