package main

import "github.com/ydb-platform/ydb-go-sdk/v3/example/internal/cli"

func main() {
	cli.Run(new(Command))
}
