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
			fluentSettings: TimeToLiveSettings{}.
				WithColumnName("a").
				WithColumnUnit(TimeToLiveUnitSeconds).
				WithMode(TimeToLiveModeValueSinceUnixEpoch).
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit: func() *TimeToLiveUnit {
					u := TimeToLiveUnitSeconds
					return &u
				}(),
			},
		},
		{
			fluentSettings: TimeToLiveSettings{}.
				WithColumnName("a").
				ModeDate().
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeDateType,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
			},
		},
		{
			fluentSettings: TimeToLiveSettings{}.
				WithColumnName("a").
				ModeSinceEpoch().
				ColumnUnitSeconds().
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit: func() *TimeToLiveUnit {
					u := TimeToLiveUnitSeconds
					return &u
				}(),
			},
		},
		{
			fluentSettings: TimeToLiveSettings{}.
				WithColumnName("a").
				ColumnUnitSeconds().
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit: func() *TimeToLiveUnit {
					u := TimeToLiveUnitSeconds
					return &u
				}(),
			},
		},
		{
			fluentSettings: TimeToLiveSettings{}.
				WithColumnName("a").
				ColumnUnitMilliseconds().
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit: func() *TimeToLiveUnit {
					u := TimeToLiveUnitMilliseconds
					return &u
				}(),
			},
		},
		{
			fluentSettings: TimeToLiveSettings{}.
				WithColumnName("a").
				ColumnUnitMicroseconds().
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit: func() *TimeToLiveUnit {
					u := TimeToLiveUnitMicroseconds
					return &u
				}(),
			},
		},
		{
			fluentSettings: TimeToLiveSettings{}.
				WithColumnName("a").
				ColumnUnitNanoseconds().
				ExpireAfter(time.Hour),
			expectedSettings: TimeToLiveSettings{
				ColumnName:         "a",
				Mode:               TimeToLiveModeValueSinceUnixEpoch,
				ExpireAfterSeconds: uint32(time.Hour.Seconds()),
				ColumnUnit: func() *TimeToLiveUnit {
					u := TimeToLiveUnitNanoseconds
					return &u
				}(),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.fluentSettings, tt.expectedSettings)
		})
	}
}
