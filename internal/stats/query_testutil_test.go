package stats_test

import (
	"reflect"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"google.golang.org/protobuf/encoding/protojson"
)

func mustQueryStatsFromJSON(json string) *Ydb_TableStats.QueryStats {
	var pb Ydb_TableStats.QueryStats
	if err := protojson.Unmarshal([]byte(json), &pb); err != nil {
		panic(err)
	}

	return &pb
}

func setQueryPhases(pb *Ydb_TableStats.QueryStats, phases []*Ydb_TableStats.QueryPhaseStats) {
	type phaseSetter interface {
		SetQueryPhases(phases []*Ydb_TableStats.QueryPhaseStats)
	}
	if s, ok := any(pb).(phaseSetter); ok {
		s.SetQueryPhases(phases)

		return
	}

	reflect.ValueOf(pb).Elem().FieldByName("QueryPhases").Set(reflect.ValueOf(phases))
}

func queryStatsWithNilPhase() *Ydb_TableStats.QueryStats {
	pb := &Ydb_TableStats.QueryStats{}
	setQueryPhases(pb, []*Ydb_TableStats.QueryPhaseStats{nil})

	return pb
}

func queryStatsWithEmptyPhase() *Ydb_TableStats.QueryStats {
	pb := &Ydb_TableStats.QueryStats{}
	setQueryPhases(pb, []*Ydb_TableStats.QueryPhaseStats{{}})

	return pb
}

// sampleQueryStatsPB returns a fully-populated QueryStats proto used by
// scalar-field tests. Two query phases let us also exercise NextPhase
// advancement and table access on more than one phase.
func sampleQueryStatsPB() *Ydb_TableStats.QueryStats {
	return mustQueryStatsFromJSON(`{
		"queryPhases": [
			{
				"durationUs": "10",
				"tableAccess": [{
					"name": "a",
					"reads": {"rows": "100", "bytes": "200"},
					"updates": {"rows": "300", "bytes": "400"},
					"deletes": {"rows": "500", "bytes": "600"},
					"partitionsCount": "700"
				}],
				"cpuTimeUs": "20",
				"affectedShards": "30",
				"literalPhase": true
			},
			{
				"durationUs": "11",
				"tableAccess": [{
					"name": "b",
					"reads": {"rows": "101", "bytes": "201"},
					"updates": {"rows": "301", "bytes": "401"},
					"deletes": {"rows": "501", "bytes": "601"},
					"partitionsCount": "701"
				}],
				"cpuTimeUs": "21",
				"affectedShards": "31",
				"literalPhase": false
			}
		],
		"compilation": {
			"fromCache": true,
			"durationUs": "123",
			"cpuTimeUs": "456"
		},
		"processCpuTimeUs": "100",
		"queryPlan": "plan",
		"queryAst": "ast",
		"totalDurationUs": "200",
		"totalCpuTimeUs": "300"
	}`)
}

// iterableQueryStatsPB has 3 phases × 3 tables: enough variety to verify
// ordering, full traversal, and early-break behavior of the range iterators.
func iterableQueryStatsPB() *Ydb_TableStats.QueryStats {
	return mustQueryStatsFromJSON(`{
		"queryPhases": [
			{
				"durationUs": "1",
				"tableAccess": [{"name": "a"}, {"name": "b"}, {"name": "c"}]
			},
			{
				"durationUs": "2",
				"tableAccess": [{"name": "d"}, {"name": "e"}, {"name": "f"}]
			},
			{
				"durationUs": "3",
				"tableAccess": [{"name": "g"}, {"name": "h"}, {"name": "i"}]
			}
		]
	}`)
}
