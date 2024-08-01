#!/bin/bash
sed 's/github.com\/ydb-platform\/ydb-go-sdk\/v3/slo/g' .golangci.yml > tests/slo/.golangci.yml
sed 's/github.com\/ydb-platform\/ydb-go-sdk\/v3/examples/g' .golangci.yml > examples/.golangci.yml
