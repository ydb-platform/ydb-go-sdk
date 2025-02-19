package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var dsn string

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
	db, err := ydb.Open(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	words, err := txWithRetries(ctx, db)
	if err != nil {
		panic(err)
	}
	
	fmt.Printf("SUCCESS: %q\n", strings.Join(words, " "))
}

func txWithRetries(ctx context.Context, db *ydb.Driver) (words []string, _ error) {
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		words = words[:0] // empty for new retry attempt

		result, err := tx.Execute(ctx, "SELECT 'execute';", nil)
		if err != nil {
			return err
		}

		if err := result.NextResultSetErr(ctx); err != nil {
			return err
		}

		if !result.NextRow() {
			return fmt.Errorf("no row")
		}

		var s string
		if err := result.Scan(&s); err != nil {
			return err
		}

		words = append(words, s)

		if err = result.Err(); err != nil {
			return err
		}

		_ = result.Close()

		result, err = tx.Execute(ctx, `
				DECLARE $word1 AS Text;
				DECLARE $word2 AS Text;
				DECLARE $word3 AS Text;

				SELECT w, ord FROM (
					SELECT $word1 AS w, 1 AS ord 
					UNION 
					SELECT $word2 AS w, 2 AS ord
					UNION 
					SELECT 'with'u AS w, 3 AS ord
					UNION 
					SELECT $word3 AS w, 4 AS ord
					UNION 
					SELECT 'using'u AS w, 5 AS ord
					UNION 
					SELECT 'db.Table().DoTx()'u AS w, 6 AS ord
				)
				ORDER BY ord;
			`,
			table.NewQueryParameters(
				table.ValueParam("$word1", types.TextValue("in")),
				table.ValueParam("$word2", types.TextValue("transaction")),
				table.ValueParam("$word3", types.TextValue("retries")),
			),
		)
		if err != nil {
			return err
		}

		defer result.Close()

		if err := result.NextResultSetErr(ctx); err != nil {
			return err
		}

		for result.NextRow() {
			var (
				word string
				ord  int
			)
			err = result.Scan(&word, &ord)
			if err != nil {
				return err
			}
			words = append(words, word)
		}

		return result.Err()
	})
	if err != nil {
		return nil, err
	}

	return words, nil
}
