package table

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestNewTimeToLiveSettings(t *testing.T) {
	t.Run("nil settings", func(t *testing.T) {
		result := NewTimeToLiveSettings(nil)
		require.Nil(t, result)
	})

	t.Run("DateTypeColumn mode", func(t *testing.T) {
		settings := &Ydb_Table.TtlSettings{
			Mode: &Ydb_Table.TtlSettings_DateTypeColumn{
				DateTypeColumn: &Ydb_Table.DateTypeColumnModeSettings{
					ColumnName:         "created_at",
					ExpireAfterSeconds: 3600,
				},
			},
		}

		result := NewTimeToLiveSettings(settings)
		require.NotNil(t, result)
		require.Equal(t, "created_at", result.ColumnName)
		require.Equal(t, uint32(3600), result.ExpireAfterSeconds)
		require.Equal(t, options.TimeToLiveModeDateType, result.Mode)
	})

	t.Run("ValueSinceUnixEpoch mode with seconds", func(t *testing.T) {
		settings := &Ydb_Table.TtlSettings{
			Mode: &Ydb_Table.TtlSettings_ValueSinceUnixEpoch{
				ValueSinceUnixEpoch: &Ydb_Table.ValueSinceUnixEpochModeSettings{
					ColumnName:         "timestamp",
					ColumnUnit:         Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_SECONDS,
					ExpireAfterSeconds: 7200,
				},
			},
		}

		result := NewTimeToLiveSettings(settings)
		require.NotNil(t, result)
		require.Equal(t, "timestamp", result.ColumnName)
		require.Equal(t, uint32(7200), result.ExpireAfterSeconds)
		require.Equal(t, options.TimeToLiveModeValueSinceUnixEpoch, result.Mode)
		require.NotNil(t, result.ColumnUnit)
		require.Equal(t, options.TimeToLiveUnitSeconds, *result.ColumnUnit)
	})

	t.Run("ValueSinceUnixEpoch mode with milliseconds", func(t *testing.T) {
		settings := &Ydb_Table.TtlSettings{
			Mode: &Ydb_Table.TtlSettings_ValueSinceUnixEpoch{
				ValueSinceUnixEpoch: &Ydb_Table.ValueSinceUnixEpochModeSettings{
					ColumnName:         "timestamp_ms",
					ColumnUnit:         Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MILLISECONDS,
					ExpireAfterSeconds: 1800,
				},
			},
		}

		result := NewTimeToLiveSettings(settings)
		require.NotNil(t, result)
		require.Equal(t, "timestamp_ms", result.ColumnName)
		require.Equal(t, uint32(1800), result.ExpireAfterSeconds)
		require.Equal(t, options.TimeToLiveModeValueSinceUnixEpoch, result.Mode)
		require.NotNil(t, result.ColumnUnit)
		require.Equal(t, options.TimeToLiveUnitMilliseconds, *result.ColumnUnit)
	})

	t.Run("ValueSinceUnixEpoch mode with microseconds", func(t *testing.T) {
		settings := &Ydb_Table.TtlSettings{
			Mode: &Ydb_Table.TtlSettings_ValueSinceUnixEpoch{
				ValueSinceUnixEpoch: &Ydb_Table.ValueSinceUnixEpochModeSettings{
					ColumnName:         "timestamp_us",
					ColumnUnit:         Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MICROSECONDS,
					ExpireAfterSeconds: 900,
				},
			},
		}

		result := NewTimeToLiveSettings(settings)
		require.NotNil(t, result)
		require.Equal(t, "timestamp_us", result.ColumnName)
		require.Equal(t, uint32(900), result.ExpireAfterSeconds)
		require.Equal(t, options.TimeToLiveModeValueSinceUnixEpoch, result.Mode)
		require.NotNil(t, result.ColumnUnit)
		require.Equal(t, options.TimeToLiveUnitMicroseconds, *result.ColumnUnit)
	})

	t.Run("ValueSinceUnixEpoch mode with nanoseconds", func(t *testing.T) {
		settings := &Ydb_Table.TtlSettings{
			Mode: &Ydb_Table.TtlSettings_ValueSinceUnixEpoch{
				ValueSinceUnixEpoch: &Ydb_Table.ValueSinceUnixEpochModeSettings{
					ColumnName:         "timestamp_ns",
					ColumnUnit:         Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_NANOSECONDS,
					ExpireAfterSeconds: 600,
				},
			},
		}

		result := NewTimeToLiveSettings(settings)
		require.NotNil(t, result)
		require.Equal(t, "timestamp_ns", result.ColumnName)
		require.Equal(t, uint32(600), result.ExpireAfterSeconds)
		require.Equal(t, options.TimeToLiveModeValueSinceUnixEpoch, result.Mode)
		require.NotNil(t, result.ColumnUnit)
		require.Equal(t, options.TimeToLiveUnitNanoseconds, *result.ColumnUnit)
	})

	t.Run("ValueSinceUnixEpoch mode with unspecified unit", func(t *testing.T) {
		settings := &Ydb_Table.TtlSettings{
			Mode: &Ydb_Table.TtlSettings_ValueSinceUnixEpoch{
				ValueSinceUnixEpoch: &Ydb_Table.ValueSinceUnixEpochModeSettings{
					ColumnName:         "timestamp_unspec",
					ColumnUnit:         Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_UNSPECIFIED,
					ExpireAfterSeconds: 300,
				},
			},
		}

		result := NewTimeToLiveSettings(settings)
		require.NotNil(t, result)
		require.Equal(t, "timestamp_unspec", result.ColumnName)
		require.Equal(t, uint32(300), result.ExpireAfterSeconds)
		require.Equal(t, options.TimeToLiveModeValueSinceUnixEpoch, result.Mode)
		require.NotNil(t, result.ColumnUnit)
		require.Equal(t, options.TimeToLiveUnitUnspecified, *result.ColumnUnit)
	})
}

func TestTimeToLiveUnit(t *testing.T) {
	t.Run("UNIT_SECONDS", func(t *testing.T) {
		unit := timeToLiveUnit(Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_SECONDS)
		require.NotNil(t, unit)
		require.Equal(t, options.TimeToLiveUnitSeconds, *unit)
	})

	t.Run("UNIT_MILLISECONDS", func(t *testing.T) {
		unit := timeToLiveUnit(Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MILLISECONDS)
		require.NotNil(t, unit)
		require.Equal(t, options.TimeToLiveUnitMilliseconds, *unit)
	})

	t.Run("UNIT_MICROSECONDS", func(t *testing.T) {
		unit := timeToLiveUnit(Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MICROSECONDS)
		require.NotNil(t, unit)
		require.Equal(t, options.TimeToLiveUnitMicroseconds, *unit)
	})

	t.Run("UNIT_NANOSECONDS", func(t *testing.T) {
		unit := timeToLiveUnit(Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_NANOSECONDS)
		require.NotNil(t, unit)
		require.Equal(t, options.TimeToLiveUnitNanoseconds, *unit)
	})

	t.Run("UNIT_UNSPECIFIED", func(t *testing.T) {
		unit := timeToLiveUnit(Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_UNSPECIFIED)
		require.NotNil(t, unit)
		require.Equal(t, options.TimeToLiveUnitUnspecified, *unit)
	})

	t.Run("unknown unit panics", func(t *testing.T) {
		require.Panics(t, func() {
			// Using an invalid unit value (not a defined constant)
			timeToLiveUnit(Ydb_Table.ValueSinceUnixEpochModeSettings_Unit(999))
		})
	})
}
