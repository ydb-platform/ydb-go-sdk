package options

import (
	"reflect"
	"testing"
	"time"
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
				WithExpireAfter(time.Hour),
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
	} {
		t.Run("", func(t *testing.T) {
			if !reflect.DeepEqual(tt.fluentSettings, tt.expectedSettings) {
				t.Errorf("unexpected ttl settings: %v, expectedSettings: %v", tt.fluentSettings, tt.expectedSettings)
			}
		})
	}
}
