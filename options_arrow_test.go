package ydb

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	queryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
)

func TestWithQueryDefaultResultFormatArrow(t *testing.T) {
	called := false
	decoder := arrow.Decoder(func(context.Context, []arrow.Column, io.Reader) ([]arrow.Batch, error) {
		called = true

		return nil, nil
	})
	driver, err := driverFromOptions(t.Context(), WithQueryDefaultResultFormatArrow(decoder))
	require.NoError(t, err)
	decode := queryConfig.New(driver.queryOptions...).DefaultArrowDecoder()
	require.NotNil(t, decode)
	_, err = decode(t.Context(), nil, nil)
	require.NoError(t, err)
	require.True(t, called)
	driver, err = driverFromOptions(t.Context(),
		WithQueryDefaultResultFormatArrow(decoder), WithQueryDefaultResultFormatArrow(nil),
	)
	require.NoError(t, err)
	require.Nil(t, queryConfig.New(driver.queryOptions...).DefaultArrowDecoder())
}
