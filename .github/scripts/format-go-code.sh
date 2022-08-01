#!/usr/bin/env bash

set -eu

FILEPATH="$1"

# https://github.com/rinchsan/gosimports
gosimports -local github.com/ydb-platform/ydb-go-sdk/v3,github.com/ydb-platform/ydb-go-genproto -w "$FILEPATH"

# https://github.com/mvdan/gofumpt
gofumpt -w "$FILEPATH"
