//go:build go1.23

package stats

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

func TestIterateOverQueryPhases(t *testing.T) {
	s := &QueryStats{
		pb: &Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					DurationUs: 1,
				},
				{
					DurationUs: 2,
				},
				{
					DurationUs: 3,
				},
			},
		},
	}
	t.Run("ImmutableIteration", func(t *testing.T) {
		for i := range make([]struct{}, 3) {
			t.Run(fmt.Sprintf("Pass#%d", i), func(t *testing.T) {
				durations := make([]uint64, 0, 3)
				for phase := range s.QueryPhases() {
					durations = append(durations, phase.pb.GetDurationUs())
				}
				require.Equal(t, []uint64{1, 2, 3}, durations)
			})
		}
	})
	t.Run("MutableIteration", func(t *testing.T) {
		durations := make([]uint64, 0, 3)
		for {
			phase, ok := s.NextPhase()
			if !ok {
				break
			}
			durations = append(durations, phase.pb.GetDurationUs())
		}
		require.Equal(t, []uint64{1, 2, 3}, durations)
		require.Equal(t, 3, s.pos)

		_, ok := s.NextPhase()
		require.False(t, ok)
	})
}
