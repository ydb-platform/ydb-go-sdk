package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
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
	nativeDriver, err := ydb.Open(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer nativeDriver.Close(ctx)

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithAutoDeclare(),
		ydb.WithQueryService(true),
	)
	if err != nil {
		panic(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	if words, err := txWithoutRetries(ctx, db); err != nil {
		panic(err)
	} else {
		fmt.Printf("SUCCESS: %q\n", strings.Join(words, " "))
	}

	if words, err := txWithRetries(ctx, db); err != nil {
		panic(err)
	} else {
		fmt.Printf("SUCCESS: %q\n", strings.Join(words, " "))
	}
}

func txWithoutRetries(ctx context.Context, db *sql.DB) (words []string, _ error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	row := tx.QueryRowContext(ctx, "SELECT 'execute';")

	var word string
	err = row.Scan(&word)
	if err != nil {
		return nil, err
	}

	words = append(words, word)

	rows, err := tx.QueryContext(ctx, `
			SELECT w, ord FROM (
				SELECT $word1 AS w, 1 AS ord 
				UNION 
				SELECT $word2 AS w, 2 AS ord
				UNION 
				SELECT 'without'u AS w, 3 AS ord
				UNION 
				SELECT $word3 AS w, 4 AS ord
			)
			ORDER BY ord;
		`,
		sql.Named("word1", "in"),
		sql.Named("word2", "transaction"),
		sql.Named("word3", "retries"),
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close() //nolint:errcheck,nolintlint

	for rows.Next() {
		var (
			word string
			ord  int
		)
		if err = rows.Scan(&word, &ord); err != nil {
			return nil, err
		}
		words = append(words, word)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return words, nil
}

func txWithRetries(ctx context.Context, db *sql.DB) (words []string, _ error) {
	err := retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		words = words[:0] // empty for new retry attempt

		row := tx.QueryRowContext(ctx, "SELECT 'execute';")

		var word string
		err := row.Scan(&word)
		if err != nil {
			return err
		}

		words = append(words, word)

		rows, err := tx.QueryContext(ctx, `
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
					SELECT 'retry.DoTx'u AS w, 6 AS ord
				)
				ORDER BY ord;
			`,
			sql.Named("word1", "in"),
			sql.Named("word2", "transaction"),
			sql.Named("word3", "retries"),
		)
		if err != nil {
			return err
		}

		defer rows.Close() //nolint:errcheck,nolintlint

		for rows.Next() {
			var (
				word string
				ord  int
			)
			if err = rows.Scan(&word, &ord); err != nil {
				return err
			}
			words = append(words, word)
		}

		return rows.Err()
	})
	if err != nil {
		return nil, err
	}

	return words, nil
}
