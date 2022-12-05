package traces

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
)

// latency creates Field "latency": time.Since(start)
func latency(start time.Time) logs.Field {
	return logs.Duration("latency", time.Since(start))
}

// version creates Field "version": meta.Version
func version() logs.Field {
	return logs.String("version", meta.Version)
}
