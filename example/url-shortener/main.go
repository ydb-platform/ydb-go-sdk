package main

import "github.com/yandex-cloud/ydb-go-sdk/example/internal/cli"

func main() {
	cli.Run(new(Command))
}
