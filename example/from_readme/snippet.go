package main

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2/connect"
	"context"
	"flag"
	"fmt"

	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/example/internal/cli"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
)

type Command struct {
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(connectCtx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	err = prepareScheme(ctx, db, params.Prefix())
	if err != nil {
		return fmt.Errorf("error on prepare scheme: %w", err)
	}

	err = prepareData(ctx, db, params.Prefix())
	if err != nil {
		return fmt.Errorf("error on prepare data: %w", err)
	}

	s, err := db.Table().CreateSession(ctx)
	if err != nil {
		return fmt.Errorf("error on create session: %w", err)
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
		`
			DECLARE $mystr AS Utf8?;

			SELECT 42 as id, $mystr as mystr;
		`,
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

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}
