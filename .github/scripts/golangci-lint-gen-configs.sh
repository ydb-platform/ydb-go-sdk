#!/bin/bash
sed 's/github.com\/ydb-platform\/ydb-go-sdk\/v3/slo/g' .golangci.yml | sed 's/path: tests\/slo/path: ./g' > tests/slo/.golangci.yml  > tests/slo/.golangci.yml
sed 's/github.com\/ydb-platform\/ydb-go-sdk\/v3/examples/g' .golangci.yml | sed 's/path: examples/path: ./g' > examples/.golangci.yml
