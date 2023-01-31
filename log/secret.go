package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logs/traces"
)

func Secret(secret string) string {
	return traces.Secret(secret)
}
