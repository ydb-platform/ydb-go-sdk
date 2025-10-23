package scanner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

func TestUnaryResult_ResultSetCount(t *testing.T) {
	t.Run("empty sets", func(t *testing.T) {
		r := &unaryResult{
			sets: []*Ydb.ResultSet{},
		}
		require.Equal(t, 0, r.ResultSetCount())
	})

	t.Run("with sets", func(t *testing.T) {
		r := &unaryResult{
			sets: []*Ydb.ResultSet{
				{},
				{},
				{},
			},
		}
		require.Equal(t, 3, r.ResultSetCount())
	})
}

func TestUnaryResult_Close(t *testing.T) {
	t.Run("close once", func(t *testing.T) {
		r := &unaryResult{}
		err := r.Close()
		require.NoError(t, err)
	})

	t.Run("close twice", func(t *testing.T) {
		r := &unaryResult{}
		_ = r.Close()
		err := r.Close()
		require.Error(t, err)
		require.ErrorIs(t, err, errAlreadyClosed)
	})
}

func TestBaseResult_CurrentResultSet(t *testing.T) {
	r := &baseResult{}
	set := r.CurrentResultSet()
	require.NotNil(t, set)
	require.Equal(t, r, set)
}

func TestBaseResult_Stats(t *testing.T) {
	t.Run("nil stats", func(t *testing.T) {
		r := &baseResult{}
		stats := r.Stats()
		// Stats() returns a value type, not a pointer, so it's never nil
		// We just verify it can be called without panic
		_ = stats
	})

	t.Run("with stats", func(t *testing.T) {
		r := &baseResult{
			stats: &Ydb_TableStats.QueryStats{
				QueryAst:  "test ast",
				QueryPlan: "test plan",
			},
		}
		stats := r.Stats()
		// Stats() returns a value type, not a pointer
		_ = stats
	})
}

func TestStreamResult_Close(t *testing.T) {
	t.Run("close successfully", func(t *testing.T) {
		closeCalled := false
		r := &streamResult{
			close: func(err error) error {
				closeCalled = true
				return nil
			},
		}
		err := r.Close()
		require.NoError(t, err)
		require.True(t, closeCalled)
	})

	t.Run("close with error", func(t *testing.T) {
		closeErr := errors.New("close error")
		r := &streamResult{
			close: func(err error) error {
				return closeErr
			},
		}
		err := r.Close()
		require.Error(t, err)
		require.ErrorIs(t, err, closeErr)
	})

	t.Run("close twice", func(t *testing.T) {
		r := &streamResult{
			close: func(err error) error {
				return nil
			},
		}
		_ = r.Close()
		err := r.Close()
		require.Error(t, err)
		require.ErrorIs(t, err, errAlreadyClosed)
	})
}

func TestStreamResult_HasNextResultSet(t *testing.T) {
	t.Run("active result", func(t *testing.T) {
		r := &streamResult{}
		require.True(t, r.HasNextResultSet())
	})

	t.Run("closed result", func(t *testing.T) {
		r := &streamResult{
			close: func(err error) error {
				return nil
			},
		}
		_ = r.Close()
		require.False(t, r.HasNextResultSet())
	})

	t.Run("result with error", func(t *testing.T) {
		r := &streamResult{}
		r.SetErr(errors.New("test error"))
		require.False(t, r.HasNextResultSet())
	})
}

func TestUnaryResult_HasNextResultSet(t *testing.T) {
	t.Run("has next set", func(t *testing.T) {
		r := &unaryResult{
			sets: []*Ydb.ResultSet{
				{},
				{},
			},
			nextSet: 0,
		}
		require.True(t, r.HasNextResultSet())
	})

	t.Run("no more sets", func(t *testing.T) {
		r := &unaryResult{
			sets: []*Ydb.ResultSet{
				{},
			},
			nextSet: 1,
		}
		require.False(t, r.HasNextResultSet())
	})

	t.Run("closed result", func(t *testing.T) {
		r := &unaryResult{
			sets: []*Ydb.ResultSet{
				{},
			},
		}
		_ = r.Close()
		require.False(t, r.HasNextResultSet())
	})
}

func TestStreamResult_NextResultSet(t *testing.T) {
	t.Run("successful next", func(t *testing.T) {
		r := &streamResult{
			recv: func(ctx context.Context) (*Ydb.ResultSet, *Ydb_TableStats.QueryStats, error) {
				return &Ydb.ResultSet{}, nil, nil
			},
		}
		require.True(t, r.NextResultSet(context.Background()))
	})

	t.Run("error on next", func(t *testing.T) {
		r := &streamResult{
			recv: func(ctx context.Context) (*Ydb.ResultSet, *Ydb_TableStats.QueryStats, error) {
				return nil, nil, errors.New("recv error")
			},
		}
		require.False(t, r.NextResultSet(context.Background()))
	})
}

func TestWithIgnoreTruncated(t *testing.T) {
	t.Run("set ignore truncated true", func(t *testing.T) {
		r := &baseResult{}
		opt := WithIgnoreTruncated(true)
		opt(r)
		require.True(t, r.valueScanner.ignoreTruncated)
	})

	t.Run("set ignore truncated false", func(t *testing.T) {
		r := &baseResult{
			valueScanner: valueScanner{
				ignoreTruncated: true,
			},
		}
		opt := WithIgnoreTruncated(false)
		opt(r)
		require.False(t, r.valueScanner.ignoreTruncated)
	})
}
