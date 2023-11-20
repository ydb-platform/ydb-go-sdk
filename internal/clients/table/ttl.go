package table

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func NewTimeToLiveSettings(settings *Ydb_Table.TtlSettings) *options.TimeToLiveSettings {
	if settings == nil {
		return nil
	}
	switch mode := settings.Mode.(type) {
	case *Ydb_Table.TtlSettings_DateTypeColumn:
		return &options.TimeToLiveSettings{
			ColumnName:         mode.DateTypeColumn.ColumnName,
			ExpireAfterSeconds: mode.DateTypeColumn.ExpireAfterSeconds,
			Mode:               options.TimeToLiveModeDateType,
		}

	case *Ydb_Table.TtlSettings_ValueSinceUnixEpoch:
		return &options.TimeToLiveSettings{
			ColumnName:         mode.ValueSinceUnixEpoch.ColumnName,
			ColumnUnit:         timeToLiveUnit(mode.ValueSinceUnixEpoch.ColumnUnit),
			ExpireAfterSeconds: mode.ValueSinceUnixEpoch.ExpireAfterSeconds,
			Mode:               options.TimeToLiveModeValueSinceUnixEpoch,
		}
	}
	return nil
}

func timeToLiveUnit(unit Ydb_Table.ValueSinceUnixEpochModeSettings_Unit) *options.TimeToLiveUnit {
	var res options.TimeToLiveUnit
	switch unit {
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_SECONDS:
		res = options.TimeToLiveUnitSeconds
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MILLISECONDS:
		res = options.TimeToLiveUnitMilliseconds
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MICROSECONDS:
		res = options.TimeToLiveUnitMicroseconds
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_NANOSECONDS:
		res = options.TimeToLiveUnitNanoseconds
	case Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_UNSPECIFIED:
		res = options.TimeToLiveUnitUnspecified
	default:
		panic("ydb: unknown Ydb unit for value since epoch")
	}
	return &res
}
