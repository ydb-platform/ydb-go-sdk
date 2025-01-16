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
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "a",
						},
						{
							Name: "b",
						},
						{
							Name: "c",
						},
					},
				},
				{
					DurationUs: 2,
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "d",
						},
						{
							Name: "e",
						},
						{
							Name: "f",
						},
					},
				},
				{
					DurationUs: 3,
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "g",
						},
						{
							Name: "h",
						},
						{
							Name: "i",
						},
					},
				},
			},
		},
	}
	t.Run("ImmutableIteration", func(t *testing.T) {
		for i := range make([]struct{}, 3) {
			t.Run(fmt.Sprintf("Pass#%d", i), func(t *testing.T) {
				durations := make([]uint64, 0, 3)
				tables := make([]string, 0, 9)
				for phase := range s.RangeQueryPhases() {
					durations = append(durations, phase.pb.GetDurationUs())
					for access := range phase.RangeTableAccess() {
						tables = append(tables, access.Name)
					}
				}
				require.Equal(t, []uint64{1, 2, 3}, durations)
				require.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, tables)
			})
		}
	})
	t.Run("MutableIteration", func(t *testing.T) {
		durations := make([]uint64, 0, 3)
		tables := make([]string, 0, 9)
		for {
			phase, ok := s.NextPhase()
			if !ok {
				break
			}
			durations = append(durations, phase.pb.GetDurationUs())
			for {
				access, ok := phase.NextTableAccess()
				if !ok {
					break
				}
				tables = append(tables, access.Name)
			}
		}
		require.Equal(t, []uint64{1, 2, 3}, durations)
		require.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, tables)
		require.Equal(t, 3, s.pos)

		_, ok := s.NextPhase()
		require.False(t, ok)
	})
}
