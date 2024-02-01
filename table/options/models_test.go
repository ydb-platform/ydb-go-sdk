package options

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeToLiveSettingsFluentModifiers(t *testing.T) {
	for _, tt := range getSecondsTTLTestCases() {
		t.Run("Seconds", func(t *testing.T) {
			require.Equal(t, tt.expectedSettings, tt.fluentSettings)
		})
	}
	for _, tt := range getMillisecondsTTLTestCases() {
		t.Run("Milliseconds", func(t *testing.T) {
			require.Equal(t, tt.expectedSettings, tt.fluentSettings)
		})
	}
	for _, tt := range getMicrosecondsTTLTestCases() {
		t.Run("Microseconds", func(t *testing.T) {
			require.Equal(t, tt.expectedSettings, tt.fluentSettings)
		})
	}
	for _, tt := range getNanosecondsTTLTestCases() {
		t.Run("Nanoseconds", func(t *testing.T) {
			require.Equal(t, tt.expectedSettings, tt.fluentSettings)
		})
	}
	for _, tt := range getDateTypeTTLTestCases() {
		t.Run("DateType", func(t *testing.T) {
			require.Equal(t, tt.expectedSettings, tt.fluentSettings)
		})
	}
}

func getSecondsTTLTestCases() []struct {
	fluentSettings   TimeToLiveSettings
	expectedSettings TimeToLiveSettings
} {
	return []struct {
		fluentSettings   TimeToLiveSettings
		expectedSettings TimeToLiveSettings
	}{
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
	}
}

func getMillisecondsTTLTestCases() []struct {
	fluentSettings   TimeToLiveSettings
	expectedSettings TimeToLiveSettings
} {
	return []struct {
		fluentSettings   TimeToLiveSettings
		expectedSettings TimeToLiveSettings
	}{
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
	}
}

func getMicrosecondsTTLTestCases() []struct {
	fluentSettings   TimeToLiveSettings
	expectedSettings TimeToLiveSettings
} {
	return []struct {
		fluentSettings   TimeToLiveSettings
		expectedSettings TimeToLiveSettings
	}{
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
	}
}

func getNanosecondsTTLTestCases() []struct {
	fluentSettings   TimeToLiveSettings
	expectedSettings TimeToLiveSettings
} {
	return []struct {
		fluentSettings   TimeToLiveSettings
		expectedSettings TimeToLiveSettings
	}{
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
	}
}

func getDateTypeTTLTestCases() []struct {
	fluentSettings   TimeToLiveSettings
	expectedSettings TimeToLiveSettings
} {
	return []struct {
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
	}
}
