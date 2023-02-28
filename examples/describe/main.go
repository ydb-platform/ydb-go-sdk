package main

import (
	"bytes"
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"path"
	"text/template"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

var (
	required = []string{"ydb"}
	dsn      string
	prefix   string

	//go:embed table.tpl
	defaultTableTemplate string
	tableTemplate        string

	ignoreDirs = map[string]struct{}{
		".sys":        {},
		".sys_health": {},
	}
)

func parseFlags() {
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
	flagSet.StringVar(&tableTemplate,
		"template", defaultTableTemplate,
		"template for print table",
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
	parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := ydb.Open(ctx, dsn,
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	if prefix == "" {
		prefix = db.Name()
	}

	t, err := template.New("").Parse(tableTemplate)
	if err != nil {
		panic(fmt.Errorf("template parse error: %w", err))
	}

	list(ctx, db, t, prefix)
}

func list(ctx context.Context, db ydb.Connection, t *template.Template, p string) {
	var dir scheme.Directory
	var err error
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		dir, err = db.Scheme().ListDirectory(ctx, p)
		return err
	}, retry.WithIdempotent(true))
	if err != nil {
		fmt.Printf("list directory '%s' failed: %v\n", p, err)
		return
	}

	for _, child := range dir.Children {
		pt := path.Join(p, child.Name)
		switch child.Type {
		case scheme.EntryDirectory, scheme.EntryDatabase:
			if _, ok := ignoreDirs[child.Name]; ok {
				continue
			}
			list(ctx, db, t, pt)

		case scheme.EntryTable:
			var desc options.Description
			err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
				desc, err = s.DescribeTable(ctx, pt)
				return err
			}, table.WithIdempotent())
			if err != nil {
				fmt.Printf("describe '%s' failed: %v\n", pt, err)
				continue
			}
			desc.Name = pt
			var buf bytes.Buffer
			if err = t.Execute(&buf, desc); err == nil {
				fmt.Println(buf.String())
			} else {
				fmt.Println(err)
			}

		default:
		}
	}
}
