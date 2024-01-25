package options

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeToLiveSettingsFluentModifiers(t *testing.T) {
	for _, tt := range []struct {
		fluentSettings   TimeToLiveSettings
		expectedSettings TimeToLiveSettings
	}{
		{
			fluentSettings: NewTTLSettings().
				ColumnDateType("a").
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeDateType,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
			},
		},
		{
			fluentSettings: NewTTLSettings().
				ColumnSeconds("a").
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit:         unitToPointer(TimeToLiveUnitSeconds),
			},
		},
		{
			fluentSettings: NewTTLSettings().
				ColumnMilliseconds("a").
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit:         unitToPointer(TimeToLiveUnitMilliseconds),
			},
		},
		{
			fluentSettings: NewTTLSettings().
				ColumnMicroseconds("a").
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit:         unitToPointer(TimeToLiveUnitMicroseconds),
			},
		},
		{
			fluentSettings: NewTTLSettings().
				ColumnNanoseconds("a").
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit:         unitToPointer(TimeToLiveUnitNanoseconds),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.expectedSettings, tt.fluentSettings)
		})
	}
}
