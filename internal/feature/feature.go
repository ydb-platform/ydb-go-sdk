package feature

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type Flag int

const (
	FeatureUnknown Flag = iota
	FeatureEnabled
	FeatureDisabled
)

func (f Flag) ToYDB() Ydb.FeatureFlag_Status {
	switch f {
	case FeatureEnabled:
		return Ydb.FeatureFlag_ENABLED
	case FeatureDisabled:
		return Ydb.FeatureFlag_DISABLED
	case FeatureUnknown:
		return Ydb.FeatureFlag_STATUS_UNSPECIFIED
	default:
		panic("ydb: unknown feature flag status")
	}
}

func FromYDB(f Ydb.FeatureFlag_Status) Flag {
	switch f {
	case Ydb.FeatureFlag_ENABLED:
		return FeatureEnabled
	case Ydb.FeatureFlag_DISABLED:
		return FeatureDisabled
	case Ydb.FeatureFlag_STATUS_UNSPECIFIED:
		return FeatureUnknown
	default:
		panic("ydb: unknown Ydb feature flag status")
	}
}
