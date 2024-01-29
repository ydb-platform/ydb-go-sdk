#!/usr/bin/env bash

set -eu

FILEPATH="$1"

gofmt -s -w "$FILEPATH"

# https://github.com/daixiang0/gci
if [[ "$FILEPATH" == *"tests/slo/"* ]]
then
  gci write --skip-generated -s standard -s default -s "prefix(slo)" "$FILEPATH"
elif [[ "$FILEPATH" == *"examples/"* ]]
then
  gci write --skip-generated -s standard -s default -s "prefix(examples)" "$FILEPATH"
else
  gci write --skip-generated -s standard -s default -s "prefix(github.com/ydb-platform/ydb-go-sdk/v3)" "$FILEPATH"
fi


# https://github.com/mvdan/gofumpt
gofumpt -w "$FILEPATH"
