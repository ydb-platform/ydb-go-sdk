package mock

import (
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
			if err := stream.Send(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet:      selectOneResultSet(),
			}.Build()); err != nil {
				return err
			}

			return status.Error(codes.Canceled, "mock: first commit canceled before exec stats")
		}

		return stream.Send(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      selectOneResultSet(),
			ExecStats:      commitExecStats(),
		}.Build())
	case executeQueryBehaviorCommitStatsDelayed:
		if err := stream.Send(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      selectOneResultSet(),
		}.Build()); err != nil {
			return err
		}

		return stream.Send(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:    Ydb.StatusIds_SUCCESS,
			ExecStats: commitExecStats(),
		}.Build())
	default:
		return stream.Send(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      selectOneResultSet(),
			ExecStats:      commitExecStats(),
		}.Build())
	}
}

func selectOneResultSet() *Ydb.ResultSet {
	return Ydb.ResultSet_builder{
		Columns: []*Ydb.Column{
			Ydb.Column_builder{
				Name: "column0",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INT32.Enum(),
				}.Build(),
			}.Build(),
		},
		Rows: []*Ydb.Value{
			Ydb.Value_builder{
				Items: []*Ydb.Value{
					Ydb.Value_builder{
						Int32Value: proto.Int32(1),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
}

func commitExecStats() *Ydb_TableStats.QueryStats {
	return Ydb_TableStats.QueryStats_builder{
		QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
			Ydb_TableStats.QueryPhaseStats_builder{
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					Ydb_TableStats.TableAccessStats_builder{
						Name: "table",
						Deletes: Ydb_TableStats.OperationStats_builder{
							Rows: 1,
							Bytes: 1,
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
}