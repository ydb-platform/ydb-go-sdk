package internal

import "github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb"

type FeatureFlag int

const (
	FeatureUnknown FeatureFlag = iota
	FeatureEnabled
	FeatureDisabled
)

func (f FeatureFlag) ToYDB() Ydb.FeatureFlag_Status {
	switch f {
	case FeatureEnabled:
		return Ydb.FeatureFlag_ENABLED
	case FeatureDisabled:
		return Ydb.FeatureFlag_DISABLED
	default:
		panic("ydb: unknown feature flag status")
	}
}

func FeatureFlagFromYDB(f Ydb.FeatureFlag_Status) FeatureFlag {
	switch f {
	case Ydb.FeatureFlag_STATUS_UNSPECIFIED:
		return FeatureUnknown
	case Ydb.FeatureFlag_ENABLED:
		return FeatureEnabled
	case Ydb.FeatureFlag_DISABLED:
		return FeatureDisabled
	default:
		panic("ydb: unknown Ydb feature flag status")
	}
}
