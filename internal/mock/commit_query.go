package mock

import (
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CommitSelectOne is the query text used by commit-query mock routing and commit regression tests.
const CommitSelectOne = "select 1"

type executeQueryBehavior int

const (
	executeQueryBehaviorDefault executeQueryBehavior = iota
	executeQueryBehaviorCommitFirstCanceledThenStatsFirstPart
	executeQueryBehaviorCommitStatsDelayed
)

func WithCommitFirstCanceledThenStatsFirstPart() ServerOption {
	return func(m *server) {
		m.executeQueryBehavior = executeQueryBehaviorCommitFirstCanceledThenStatsFirstPart
	}
}

func WithCommitStatsDelayed() ServerOption {
	return func(m *server) {
		m.executeQueryBehavior = executeQueryBehaviorCommitStatsDelayed
	}
}

func isCommitQuery(req *Ydb_Query.ExecuteQueryRequest) bool {
	if req == nil || req.GetQueryContent() == nil {
		return false
	}

	text := strings.TrimSpace(strings.ToLower(req.GetQueryContent().GetText()))
	if text != CommitSelectOne {
		return false
	}

	if req.GetStatsMode() != Ydb_Query.StatsMode_STATS_MODE_BASIC {
		return false
	}

	return req.GetTxControl().GetCommitTx()
}

func (m *querySrv) executeCommitQuery(
	_ *Ydb_Query.ExecuteQueryRequest,
	stream Ydb_Query_V1.QueryService_ExecuteQueryServer,
) error {
	switch m.mock.executeQueryBehavior {
	case executeQueryBehaviorCommitFirstCanceledThenStatsFirstPart:
		call := m.mock.commitQueryCalls.Add(1)
		if call == 1 {
			if err := stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet:      selectOneResultSet(),
			}); err != nil {
				return err
			}

			return status.Error(codes.Canceled, "mock: first commit canceled before exec stats")
		}

		return stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      selectOneResultSet(),
			ExecStats:      docapiCommitExecStats(),
		})
	case executeQueryBehaviorCommitStatsDelayed:
		if err := stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      selectOneResultSet(),
		}); err != nil {
			return err
		}

		return stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
			Status:    Ydb.StatusIds_SUCCESS,
			ExecStats: docapiCommitExecStats(),
		})
	default:
		return stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      selectOneResultSet(),
			ExecStats:      docapiCommitExecStats(),
		})
	}
}

func selectOneResultSet() *Ydb.ResultSet {
	return &Ydb.ResultSet{
		Columns: []*Ydb.Column{
			{
				Name: "column0",
				Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
			},
		},
		Rows: []*Ydb.Value{
			{Items: []*Ydb.Value{{Value: &Ydb.Value_Int32Value{Int32Value: 1}}}},
		},
	}
}

func docapiCommitExecStats() *Ydb_TableStats.QueryStats {
	return &Ydb_TableStats.QueryStats{
		QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
			{
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					{
						Name:    "table",
						Deletes: &Ydb_TableStats.OperationStats{Rows: 1, Bytes: 1},
					},
				},
			},
		},
	}
}
