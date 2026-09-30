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
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

var (
	dsn    string
	prefix string
)

func init() { //nolint:gochecknoinits
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
		environ.WithEnvironCredentials(),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	prefix = path.Join(db.Name(), prefix)

	tablePath := path.Join(prefix, "decimals")
	err = db.Query().Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id Uint32,
			value Decimal(22, 9),
			PRIMARY KEY (id)
		)`, "`"+tablePath+"`"), query.WithIdempotent())
	if err != nil {
		panic(err)
	}

	x := big.NewInt(42 * 1000000000)
	x.Mul(x, big.NewInt(2))
	parsedDecimal, err := types.DecimalValueFromString("42.00", 22, 9)
	if err != nil {
		panic(err)
	}
	err = db.Query().Exec(ctx, render(writeQuery, templateConfig{
		TablePathPrefix: prefix,
	}), query.WithParameters(ydb.ParamsBuilder().Param("$decimals").Any(
		types.ListValue(
			types.StructValue(
				types.StructFieldValue("id", types.Uint32Value(42)),
				types.StructFieldValue("value", types.DecimalValueFromBigInt(x, 22, 9)),
			),
			types.StructValue(
				types.StructFieldValue("id", types.Uint32Value(43)),
				types.StructFieldValue("value", parsedDecimal),
			),
		),
	).Build()), query.WithIdempotent())
	if err != nil {
		panic(err)
	}

	var values []string
	err = db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		res, err := session.Query(ctx, render(readQuery, templateConfig{
			TablePathPrefix: prefix,
		}))
		if err != nil {
			return err
		}
		defer func() { _ = res.Close(ctx) }()

		var attemptValues []string
		for resultSet, err := range res.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range resultSet.Rows(ctx) {
				if err != nil {
					return err
				}
				var decimal *types.Decimal
				if err = row.Scan(&decimal); err != nil {
					return err
				}
				attemptValues = append(attemptValues, decimal.String())
			}
		}
		values = attemptValues

		return nil
	}, query.WithIdempotent())
	if err != nil {
		panic(err)
	}
	for _, value := range values {
		fmt.Println(value)
	}
}
