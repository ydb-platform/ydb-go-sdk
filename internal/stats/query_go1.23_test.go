//go:build go1.23

package stats_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

// iterableQueryStatsPB has 3 phases × 3 tables: enough variety to verify
// ordering, full traversal, and early-break behavior of the range iterators.
func iterableQueryStatsPB() *Ydb_TableStats.QueryStats {
	return Ydb_TableStats.QueryStats_builder{
		QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
			Ydb_TableStats.QueryPhaseStats_builder{
				DurationUs: 1,
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					Ydb_TableStats.TableAccessStats_builder{Name: "a"}.Build(),
					Ydb_TableStats.TableAccessStats_builder{Name: "b"}.Build(),
					Ydb_TableStats.TableAccessStats_builder{Name: "c"}.Build(),
				},
			}.Build(),
			Ydb_TableStats.QueryPhaseStats_builder{
				DurationUs: 2,
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					Ydb_TableStats.TableAccessStats_builder{Name: "d"}.Build(),
					Ydb_TableStats.TableAccessStats_builder{Name: "e"}.Build(),
					Ydb_TableStats.TableAccessStats_builder{Name: "f"}.Build(),
				},
			}.Build(),
			Ydb_TableStats.QueryPhaseStats_builder{
				DurationUs: 3,
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					Ydb_TableStats.TableAccessStats_builder{Name: "g"}.Build(),
					Ydb_TableStats.TableAccessStats_builder{Name: "h"}.Build(),
					Ydb_TableStats.TableAccessStats_builder{Name: "i"}.Build(),
				},
			}.Build(),
		},
	}.Build()
}

func TestQueryPhasesIterator(t *testing.T) {
	t.Run("WalksAllPhasesAndTables", func(t *testing.T) {
		s := stats.FromQueryStats(iterableQueryStatsPB())

		var (
			durations []time.Duration
			tables    []string
		)
		for phase := range s.QueryPhases() {
			durations = append(durations, phase.Duration())
			for access := range phase.TableAccess() {
				tables = append(tables, access.Name)
			}
		}

		require.Equal(t, []time.Duration{us(1), us(2), us(3)}, durations)
		require.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, tables)
	})

	t.Run("IsRestartable", func(t *testing.T) {
		// The range iterator must be independent of NextPhase/NextTableAccess
		// cursors: calling it multiple times must yield the same sequence.
		s := stats.FromQueryStats(iterableQueryStatsPB())

		for pass := range 3 {
			var tables []string
			for phase := range s.QueryPhases() {
				for access := range phase.TableAccess() {
					tables = append(tables, access.Name)
				}
			}
			require.Equalf(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, tables, "pass #%d", pass)
		}
	})

	t.Run("BreakStopsPhaseIteration", func(t *testing.T) {
		s := stats.FromQueryStats(iterableQueryStatsPB())

		var durations []time.Duration
		for phase := range s.QueryPhases() {
			durations = append(durations, phase.Duration())
			if len(durations) == 2 {
				break
			}
		}

		require.Equal(t, []time.Duration{us(1), us(2)}, durations)
	})

	t.Run("BreakStopsTableAccessIteration", func(t *testing.T) {
		s := stats.FromQueryStats(iterableQueryStatsPB())

		phase, ok := s.NextPhase()
		require.True(t, ok)

		var tables []string
		for access := range phase.TableAccess() {
			tables = append(tables, access.Name)
			if len(tables) == 2 {
				break
			}
		}

		require.Equal(t, []string{"a", "b"}, tables)
	})

	t.Run("EmptyPhasesYieldsNothing", func(t *testing.T) {
		s := stats.FromQueryStats(&Ydb_TableStats.QueryStats{})

		var count int
		for range s.QueryPhases() {
			count++
		}

		require.Zero(t, count)
	})
}

func TestNextPhaseAndIteratorCursorsAreIndependent(t *testing.T) {
	// Mutating cursors (NextPhase, NextTableAccess) advance internal state;
	// the range iterator must NOT share that state and must always start from
	// the beginning of the underlying slice. This protects callers that mix
	// the two styles (e.g. peek with NextPhase then iterate with range).
	s := stats.FromQueryStats(iterableQueryStatsPB())

	first, ok := s.NextPhase()
	require.True(t, ok)
	require.Equal(t, us(1), first.Duration())

	var durations []time.Duration
	for phase := range s.QueryPhases() {
		durations = append(durations, phase.Duration())
	}

	require.Equal(t, []time.Duration{us(1), us(2), us(3)}, durations)
}
