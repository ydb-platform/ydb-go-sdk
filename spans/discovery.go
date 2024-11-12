package spans

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func discovery(adapter Adapter) (t trace.Discovery) {
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(discovery trace.DiscoveryDiscoverDoneInfo) {
		if adapter.Details()&trace.DiscoveryEvents != 0 {
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("address", info.Address),
				kv.String("database", info.Database),
			)

			return func(info trace.DiscoveryDiscoverDoneInfo) {
				if info.Error != nil {
					start.Error(info.Error)
				} else {
					endpoints := make([]string, len(info.Endpoints))
					for i, e := range info.Endpoints {
						endpoints[i] = e.String()
					}
					start.Log(fmt.Sprintf("endpoints=%v", endpoints))
				}
				start.End()
			}
		}

		return nil
	}

	return t
}
