package main

import (
	"slo/native/query/internal"
)

var (
	label   string
	jobName string
)

func main() {
	internal.Main(label, jobName)
}
