package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/example/internal/cli"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
)

type Command struct {
	config func(cli.Parameters) *ydb.DriverConfig
	tls    func() *tls.Config
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	dialer := &ydb.Dialer{
		DriverConfig: cmd.config(params),
		TLSConfig:    cmd.tls(),
		Timeout:      time.Second,
	}
	driver, err := dialer.Dial(ctx, params.Endpoint)
	/**
	Snippet has this dialer instead

	```go
	dialer := &ydb.Dialer{
		DriverConfig: &ydb.DriverConfig{
			Database: "/ru/home/username/db",
			Credentials: ydb.AuthTokenCredentials{
				AuthToken: os.Getenv("YDB_TOKEN"),
			},
		},
		TLSConfig:    &tls.Config{},
		Timeout:      time.Second,
	}
	driver, err := dialer.Dial(ctx, "ydb-ru.yandex.net:2135")
	```
	*/
	if err != nil {
		return err // handle error
	}
	tc := table.Client{
		Driver: driver,
	}
	s, err := tc.CreateSession(ctx)
	if err != nil {
		return err // handle error
	}
	defer func() {
		_ = s.Close(ctx)
	}()

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
		`--!syntax_v1
DECLARE $mystr AS Utf8?; SELECT 42 as id, $mystr as mystr`,
		table.NewQueryParameters(
			table.ValueParam("$mystr", ydb.OptionalValue(ydb.UTF8Value("test"))),
		),
	)
	if err != nil {
		return err // handle error
	}
	// Scan for received values within the result set(s).
	// Note that Next*() methods report about success of advancing, while
	// res.Err() reports the reason of last unsuccessful one.
	for res.NextSet() {
		for res.NextRow() {
			// Suppose our "users" table has two rows: id and age.
			// Thus, current row will contain two appropriate items with
			// exactly the same order.
			//
			// Note the O*() getters. "O" stands for Optional. That is,
			// currently, all columns in tables are optional types.
			res.NextItem()
			id := res.Int32()

			res.NextItem()
			myStr := res.OUTF8()

			// Note that any value getter (such that OUTF8() and Int32()
			// above) may fail the result scanning. When this happens, getter
			// function returns zero value of requested type and marks result
			// scanning as failed, preventing any further scanning. In this
			// case res.Err() will return the cause of fail.
			if res.Err() == nil {
				// do something with data
				fmt.Printf("got id %v, got mystr: %v\n", id, myStr)
			} else {
				return res.Err() // handle error
			}
		}
	}
	if res.Err() != nil {
		return res.Err() // handle error
	}

	return nil
}

func (cmd *Command) ExportFlags(ctx context.Context, flag *flag.FlagSet) {
	cmd.config = cli.ExportDriverConfig(ctx, flag)
	cmd.tls = cli.ExportTLSConfig(flag)
}
