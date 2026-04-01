package options

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

func QueryStatsMode(mode stats.Mode) StatsMode {
	switch mode {
	case stats.ModeBasic:
		return StatsModeBasic
	case stats.ModeFull:
		return StatsModeFull
	case stats.ModeProfile:
		return StatsModeProfile
	default:
		return StatsModeNone
	}
}
