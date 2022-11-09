package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
)

func Secret(secret string) string {
	return structural.Secret(secret)
}
