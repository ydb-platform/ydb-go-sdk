#!/usr/bin/env bash

set -eu

FILEPATH="$1"

gofmt -s -w "$FILEPATH"

# https://github.com/rinchsan/gosimports
if [[ "$FILEPATH" == *"tests/slo/"* ]]
then
  gosimports -local slo -w "$FILEPATH"
else
  gosimports -local github.com/ydb-platform/ydb-go-sdk/v3 -w "$FILEPATH"
fi


# https://github.com/mvdan/gofumpt
gofumpt -w "$FILEPATH"
