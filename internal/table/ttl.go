package table

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func NewTimeToLiveSettings(settings *Ydb_Table.TtlSettings) *options.TimeToLiveSettings {
	if settings == nil {
		return nil
	}
	switch settings.WhichMode() {
	case Ydb_Table.TtlSettings_DateTypeColumn_case:
		return &options.TimeToLiveSettings{
			ColumnName:         settings.GetDateTypeColumn().GetColumnName(),
			ExpireAfterSeconds: settings.GetDateTypeColumn().GetExpireAfterSeconds(),
			Mode:               options.TimeToLiveModeDateType,
		}

	case Ydb_Table.TtlSettings_ValueSinceUnixEpoch_case:
		return &options.TimeToLiveSettings{
			ColumnName:         settings.GetValueSinceUnixEpoch().GetColumnName(),
			ColumnUnit:         timeToLiveUnit(settings.GetValueSinceUnixEpoch().GetColumnUnit()),
			ExpireAfterSeconds: settings.GetValueSinceUnixEpoch().GetExpireAfterSeconds(),
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
