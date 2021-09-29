package options

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/feature"
)

type FeatureFlag = feature.Flag

const (
	FeatureEnabled  = feature.FeatureEnabled
	FeatureDisabled = feature.FeatureDisabled
)
