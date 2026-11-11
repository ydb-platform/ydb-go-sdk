package topicwriterinternal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestWriterReconnectorConfig_ConnectTimeout(t *testing.T) {
	for _, tt := range []struct {
		name             string
		operationTimeout time.Duration
		options          []PublicWriterOption
		want             time.Duration
	}{
		{name: "Default", want: time.Minute},
		{name: "OperationTimeout", operationTimeout: 2 * time.Minute, want: 2 * time.Minute},
		{
			name:             "ExplicitOverride",
			operationTimeout: 2 * time.Minute,
			options:          []PublicWriterOption{WithConnectTimeout(time.Second)},
			want:             time.Second,
		},
		{
			name:             "ZeroUsesOperationTimeout",
			operationTimeout: 2 * time.Minute,
			options:          []PublicWriterOption{WithConnectTimeout(0)},
			want:             2 * time.Minute,
		},
		{name: "ZeroUsesDefault", options: []PublicWriterOption{WithConnectTimeout(0)}, want: time.Minute},
		{
			name:    "ExplicitInfinite",
			options: []PublicWriterOption{WithConnectTimeout(value.InfiniteDuration)},
			want:    value.InfiniteDuration,
		},
		{name: "Negative", options: []PublicWriterOption{WithConnectTimeout(-time.Second)}, want: -time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var common config.Common
			config.SetOperationTimeout(&common, tt.operationTimeout)
			options := append([]PublicWriterOption{WithCommonConfig(common)}, tt.options...)
			require.Equal(t, tt.want, NewWriterReconnectorConfig(options...).connectTimeout)
		})
	}
}
