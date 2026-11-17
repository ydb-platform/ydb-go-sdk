package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"

	queryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
)

func TestWithQueryProtoWireDecoding(t *testing.T) {
	for _, tt := range []struct {
		name string
		opts []Option
		want bool
	}{
		{name: "default"},
		{name: "enabled", opts: []Option{WithQueryProtoWireDecoding(true)}, want: true},
		{name: "disabled", opts: []Option{WithQueryProtoWireDecoding(false)}},
		{
			name: "last option wins",
			opts: []Option{WithQueryProtoWireDecoding(true), WithQueryProtoWireDecoding(false)},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			driver, err := driverFromOptions(t.Context(), tt.opts...)
			require.NoError(t, err)

			require.Equal(t, tt.want, queryConfig.New(driver.queryOptions...).ProtoWireDecoding())
		})
	}
}
