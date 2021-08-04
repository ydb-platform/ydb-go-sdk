package main

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2/example/internal/cli"
)


func main() {
	cli.Run(new(Command))
}
