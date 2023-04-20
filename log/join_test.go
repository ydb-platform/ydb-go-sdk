package log

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_join(t *testing.T) {
	for _, tt := range []struct {
		ss     []string
		join   string
		maxLen int
	}{
		{
			ss:     []string{"a", "b", "c", "d", "e", "f", "g", "h"},
			join:   "a.b.c.d.e.f.g.h",
			maxLen: 15,
		},
		{
			ss:     []string{"this", "is", "too", "long", "string"},
			join:   "t.i.t.l.string",
			maxLen: 15,
		},
		{
			ss:     []string{"ydb", "driver", "repeater", "wake", "up"},
			join:   "y.d.r.wake.up",
			maxLen: 15,
		},
		{
			ss:     []string{"ydb", "driver", "repeater", "wake", "up"},
			join:   "y.d.repeater.wake.up",
			maxLen: 20,
		},
		{
			ss:     []string{"ydb", "driver", "repeater", "wake", "up"},
			join:   "ydb.driver.repeater.wake.up",
			maxLen: 0,
		},
		{
			ss:     []string{"ydb", "driver", "credentials", "get"},
			join:   "y.driver.credentials.get",
			maxLen: 24,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.join, joinScope(tt.ss, tt.maxLen))
		})
	}
}
