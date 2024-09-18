//go:build go1.23

package sugar_test

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	internalQuery "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestUnmarshalRows(t *testing.T) {
	rows := []*internalQuery.Row{
		newRow(432, "test-1"),
		newRow(22, "2-test"),
	}

	expected := []rowTestStruct{
		{
			ID:  432,
			Str: "test-1",
		},
		{
			ID:  22,
			Str: "2-test",
		},
	}

	t.Run("OK", func(t *testing.T) {
		rowsIter := xiter.Seq2[query.Row, error](func(yield func(query.Row, error) bool) {
			if !yield(rows[0], nil) {
				return
			}
			if !yield(rows[1], nil) {
				return
			}
		})
		index := 0
		for row, err := range sugar.UnmarshalRows[rowTestStruct](rowsIter) {
			require.Equal(t, expected[index], row)
			require.NoError(t, err) // unmarshaler must hide io.EOF error - it is signal about all row readed
			index++
		}
		require.Equal(t, 2, index)
	})
	t.Run("OK_EOF", func(t *testing.T) {
		rowsIter := xiter.Seq2[query.Row, error](func(yield func(query.Row, error) bool) {
			if !yield(rows[0], nil) {
				return
			}
			if !yield(rows[1], nil) {
				return
			}
			if !yield(nil, io.EOF) {
				return
			}
		})
		index := 0
		for row, err := range sugar.UnmarshalRows[rowTestStruct](rowsIter) {
			require.Equal(t, expected[index], row)
			require.NoError(t, err)
			index++
		}
		require.Equal(t, 2, index)
	})
	t.Run("Error", func(t *testing.T) {
		testErr := errors.New("test")
		rowsIter := xiter.Seq2[query.Row, error](func(yield func(query.Row, error) bool) {
			if !yield(rows[0], nil) {
				return
			}
			if !yield(rows[1], nil) {
				return
			}
			if !yield(nil, testErr) {
				return
			}

			t.Fatal("unmarshaler must be stop after first error")
		})
		index := 0
		errorCount := 0
		var resErr error
		for row, err := range sugar.UnmarshalRows[rowTestStruct](rowsIter) {
			if err == nil {
				require.Equal(t, expected[index], row)
				index++
			} else {
				errorCount++
				resErr = err
			}
		}
		require.Equal(t, 2, index)
		require.Equal(t, 1, errorCount) // unmarhaler must stop after first error
		require.ErrorIs(t, resErr, testErr)
	})
}
