package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
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
	err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		words = words[:0] // empty for new retry attempt

		row, err := tx.QueryRow(ctx, "SELECT 'execute';")
		if err != nil {
			return err
		}

		var s string
		if err := row.Scan(&s); err != nil {
			return err
		}

		words = append(words, s)

		rows, err := tx.QueryResultSet(ctx, `
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
					SELECT 'db.Query().DoTx()'u AS w, 6 AS ord
				)
				ORDER BY ord;
			`,
			query.WithParameters(
				ydb.ParamsFromMap(map[string]interface{}{
					"$word1": "in",
					"$word2": "transaction",
					"$word3": "retries",
				}),
			),
		)
		if err != nil {
			return err
		}
		for row := range rows.Rows(ctx) {
			var (
				word string
				ord  int
			)
			err = row.Scan(&word, &ord)
			if err != nil {
				return err
			}
			words = append(words, word)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return words, nil
}
