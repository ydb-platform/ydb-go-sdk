package ydb

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	queryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
)

func TestWithQueryDefaultResultFormatArrow(t *testing.T) {
	called := false
	reader := &mockArrowOptionReader{}
	newReader := func(_ io.Reader, opts ...int) (*mockArrowOptionReader, error) {
		require.Equal(t, []int{42}, opts)
		called = true

		return reader, nil
	}
	driver, err := driverFromOptions(t.Context(), WithQueryDefaultResultFormatArrow(newReader, 42))
	require.NoError(t, err)
	decode := queryConfig.New(driver.queryOptions...).DefaultArrowDecoder()
	require.NotNil(t, decode)
	_, err = decode(t.Context(), nil, nil)
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, 1, reader.releases)
	var disabledReader func(io.Reader, ...int) (*mockArrowOptionReader, error)
	driver, err = driverFromOptions(t.Context(),
		WithQueryDefaultResultFormatArrow(newReader, 42), WithQueryDefaultResultFormatArrow(disabledReader),
	)
	require.NoError(t, err)
	require.Nil(t, queryConfig.New(driver.queryOptions...).DefaultArrowDecoder())
}

type mockArrowOptionReader struct{ releases int }

func (r *mockArrowOptionReader) Read() (arrow.Record[arrow.Array], error) { return nil, io.EOF }
func (r *mockArrowOptionReader) Release()                                 { r.releases++ }
