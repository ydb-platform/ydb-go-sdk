package internal

import "github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb"

type FeatureFlag int

const (
	FeatureUnknown FeatureFlag = iota
	FeatureEnabled
	FeatureDisabled
)

func FeatureFlagToYDB(f FeatureFlag) Ydb.FeatureFlag_Status {
	switch f {
	case FeatureEnabled:
		return Ydb.FeatureFlag_ENABLED
	case FeatureDisabled:
		return Ydb.FeatureFlag_DISABLED
	default:
		panic("ydb: unknown feature flag status")
	}
}
