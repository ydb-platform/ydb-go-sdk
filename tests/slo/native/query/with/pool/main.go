package main

import (
	"os"

	"slo/native/query/internal"
)

var (
	label   string
	jobName string
)

func main() {
	if err := os.Setenv("YDB_GO_SDK_QUERY_SERVICE_USE_SESSION_POOL", "true"); err != nil {
		panic(err)
	}
	internal.Main(label, jobName)
}
