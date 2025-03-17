package bind

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestWideTimeTypesBind(t *testing.T) {
	for _, tt := range []struct {
		bind   Bind
		sql    string
		args   []any
		yql    string
		params []any
		err    error
	}{
		{
			bind: noopBind{},
			sql:  `SELECT ?, ?`,
			args: []any{
				100,
				200,
			},
			yql: `SELECT ?, ?`,
			params: []any{
				100,
				200,
			},
		},
		{
			bind: noopBind{},
			sql:  `SELECT ?, ?`,
			args: []any{
				time.Unix(123, 456),
				200,
			},
			yql: `SELECT ?, ?`,
			params: []any{
				time.Unix(123, 456),
				200,
			},
		},
		{
			bind: noopBind{},
			sql:  `SELECT ?, ?`,
			args: []any{
				time.Duration(123) * time.Millisecond,
				200,
			},
			yql: `SELECT ?, ?`,
			params: []any{
				time.Duration(123) * time.Millisecond,
				200,
			},
		},
		{
			bind: WideTimeTypes(true),
			sql:  `SELECT ?, ?`,
			args: []any{
				100,
				200,
			},
			yql: `SELECT ?, ?`,
			params: []any{
				100,
				200,
			},
		},
		{
			bind: WideTimeTypes(true),
			sql:  `SELECT ?, ?`,
			args: []any{
				time.Unix(123, 456),
				200,
			},
			yql: `SELECT ?, ?`,
			params: []any{
				value.Timestamp64ValueFromTime(time.Unix(123, 456)),
				200,
			},
		},
		{
			bind: WideTimeTypes(true),
			sql:  `SELECT ?, ?`,
			args: []any{
				time.Duration(123) * time.Millisecond,
				200,
			},
			yql: `SELECT ?, ?`,
			params: []any{
				value.Interval64ValueFromDuration(time.Duration(123) * time.Millisecond),
				200,
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			yql, params, err := tt.bind.ToYdb(tt.sql, tt.args...)
			if tt.err != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.yql, yql)
				require.Equal(t, tt.params, params)
			}
		})
	}
}
