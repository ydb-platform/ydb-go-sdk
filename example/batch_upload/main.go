package main

import "github.com/YandexDatabase/ydb-go-sdk/v2/example/internal/cli"

func main() {
	cli.Run(new(Command))
}
