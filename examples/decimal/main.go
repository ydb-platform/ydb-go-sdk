package main

import (
	"context"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	dsn    string
	prefix string
)

func init() {
	required := []string{"ydb"}
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}
	flagSet.StringVar(&dsn,
		"ydb", "",
		"YDB connection string",
	)
	flagSet.StringVar(&prefix,
		"prefix", "",
		"tables prefix",
	)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		flagSet.Usage()
		os.Exit(1)
	}
	flagSet.Visit(func(f *flag.Flag) {
		for i, arg := range required {
			if arg == f.Name {
				required = append(required[:i], required[i+1:]...)
			}
		}
	})
	if len(required) > 0 {
		fmt.Printf("\nSome required options not defined: %v\n\n", required)
		flagSet.Usage()
		os.Exit(1)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := ydb.Open(ctx, dsn,
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	prefix = path.Join(db.Name(), prefix)

	tablePath := path.Join(prefix, "decimals")
	err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, tablePath,
				options.WithColumn("id", types.Optional(types.TypeUint32)),
				options.WithColumn("value", types.Optional(types.DefaultDecimal)),
				options.WithPrimaryKeyColumn("id"),
			)
		},
	)
	if err != nil {
		panic(err)
	}

	err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			txc := table.TxControl(
				table.BeginTx(
					table.WithSerializableReadWrite(),
				),
				table.CommitTx(),
			)

			x := big.NewInt(42 * 1000000000)
			x.Mul(x, big.NewInt(2))

			_, _, err = s.Execute(ctx, txc, render(writeQuery, templateConfig{
				TablePathPrefix: prefix,
			}), table.NewQueryParameters(
				table.ValueParam("$decimals",
					types.ListValue(
						types.StructValue(
							types.StructFieldValue("id", types.Uint32Value(42)),
							types.StructFieldValue("value", types.DecimalValueFromBigInt(x, 22, 9)),
						),
					),
				),
			))
			if err != nil {
				return err
			}

			_, res, err := s.Execute(ctx, txc, render(readQuery, templateConfig{
				TablePathPrefix: prefix,
			}), nil)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			var p *types.Decimal
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					err = res.Scan(&p)
					if err != nil {
						return err
					}

					fmt.Println(p.String())
				}
			}
			return res.Err()
		},
	)
	if err != nil {
		panic(err)
	}
}
