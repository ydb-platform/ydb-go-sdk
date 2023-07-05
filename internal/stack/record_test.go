package stack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecord(t *testing.T) {
	for _, tt := range []struct {
		act string
		exp string
	}{
		{
			act: Record(0),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestRecord(record_test.go:15)",
		},
		{
			act: func() string {
				return Record(1)
			}(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestRecord(record_test.go:21)",
		},
		{
			act: func() string {
				return func() string {
					return Record(2)
				}()
			}(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestRecord(record_test.go:29)",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.exp, tt.act)
		})
	}
}
