# ydb

> YDB API client written in Go.

[godoc](https://godoc.org/github.com/yandex-cloud/ydb-go-sdk/)

## Overview

Currently package ydb provides **scheme** and **table** API client
implementations for YDB.

## Usage

The straightforward example of querying data may looks similar to this:

```go
	driver, err := ydb.Dial(ctx, "ydb-ru.yandex.net:2135", &ydb.DriverConfig{
		Database: "/ru/home/username/db",
		Credentials: ydb.AuthTokenCredentials{
			AuthToken: os.Getenv("YDB_TOKEN"),
		},
	})
	if err != nil {
		// handle error
	}
	tc := table.Client{
		Driver: driver,
	}
	s, err := tc.CreateSession(ctx)
	if err != nil {
		// handle error
	}
	defer s.Close()

	// Prepare transaction control for upcoming query execution.
	// NOTE: result of TxControl() may be reused.
	txc := table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)
	// Execute text query without preparation and with given "autocommit"
	// transaction control. That is, transaction will be commited without
	// additional calls. Notice the "_" unused variable – it stands for created
	// transaction during execution, but as said above, transaction is commited
	// for us and we do not want to do anything with it.
	_, res, err := s.Execute(ctx, txc,
		table.TextDataQuery(
			`DECLARE $id AS "Utf8"; SELECT * FROM users WHERE user_id=$id`,
		),
		table.NewQueryParameters(
			table.ValueParam("$id", ydb.UTF8Value("42")),
		),
	)
	if err != nil {
		// handle error
	}
	// Scan for received values within the result set(s).
	// Note that Next*() methods report about success of advancing, while
	// res.Err() reports the reason of last unsuccessful one.
	var users []user
	for res.NextSet() {
		for res.NextRow() {
			// Suppose our "users" table has two rows: id and age.
			// Thus, current row will contain two appropriate items with
			// exactly the same order.
			//
			// Note the O*() getters. "O" stands for Optional. That is,
			// currently, all columns in tables are optional types.
			res.NextItem()
			id := res.OUTF8()

			res.NextItem()
			age := res.OUint64()

			// Note that any value getter (such that OUTF8() and OUint64()
			// above) may fail the result scanning. When this happens, getter 
			// function returns zero value of requested type and marks result 
			// scanning as failed, preventing any further scanning. In this 
			// case res.Err() will return the cause of fail.
			if res.Err() == nil {
				users = append(users, user{
					id:  id,
					age: age,
				})
			}
		}
	}
	if res.Err() != nil {
		// handle error
	}
```

YDB sessions may become staled and appropriate error will be returned. To
reduce boilerplate overhead for such cases `ydb` provides generic retry logic:

```go
	// Prepare session pool to be used during retries.
	sp := table.SessionPool{
		SizeLimit:          -1,          // No limits for pool size.
		KeepAliveBatchSize: -1,          // Keep alive as much as possible number of sessions.
		IdleThreshold:      time.Second, // Keep alive idle session every second.
		Builder:            &tc,         // Create new sessions within tc.
	}
	defer sp.Reset(ctx) // Close all sessions within pool.
	
	var res *table.Result
	// Retry() will call given OperationFunc with the following invariants:
	//  - previous operation failed with retriable error;
	//  - number of retries is under the limit (default to 10, see table.Repeater docs);
	//
	// Note that in case of prepared statements call to Prepare() must be made
	// inside the Operation body.
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.Execute(...)
			return
		}),
	)
```

That is, instead of manually creation of `table.Session`, we give a
`SessionPool` such responsibility. It holds instances of active sessions and
"pings" them periodically to keep them alive.

See `table.Repeater` docs for more information about retrying options.

### database/sql driver

There is a `database/sql` driver for the sql-based applications or those which
by some reasons need to use additional layer of absctraction between user code
and storage backend.

For more information please see the docs of `ydb/ydbsql` package which provides
`database/sql` driver implementation.

## Examples

More examples are listed in `ydb/examples` directory.

## Arcadia development notes

This library uses generation of Go code from protobuf specs. That is, in case
of development/debugging processes it is helpful to have result of that
generation in the directory tree. To achieve this simply run this command:

```
$ ya make --add-result go kikimr/public/sdk/go/ydb
```

This will create symlinks inside directories to the result of generation.

Also, this step may be automated via this command:
```
$ mkdir -p junk/$USERNAME
$ ya gen-config > junk/$USERNAME/ya.conf
$ sed -i "" 's/# add_result = \[\]/add_result = ["go"]/g' junk/$USERNAME/ya.conf
```

