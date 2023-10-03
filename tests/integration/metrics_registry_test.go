//go:build integration && !go1.18
// +build integration,!go1.18

package integration

import (
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func withMetrics(t *testing.T, details trace.Details, interval time.Duration) ydb.Option {
	return nil // nop
}
