package main

import (
	"context"
	"flag"
	"fmt"

	//"github.com/ydb-platform/ydb-go-examples/pkg/cli"
	"github.com/ydb-platform/ydb-go-sdk/v3/connect"
	"os"
)

func main() {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	link := ""
	flagSet.StringVar(&link,
		"ydb", "",
		"YDB connection string",
	)

	_ = flagSet.Parse(os.Args[1:])

	connectParams := connect.MustConnectionString(link)
	err := Run(context.Background(), connectParams)
	if err != nil {
		fmt.Print(err)
	}
}
