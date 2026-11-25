package query

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	publicquery "github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQuerySavedRowsSupportConcurrentScans(t *testing.T) {
	server := concurrentWireResultServer()
	conn := newWireResultConn(t, server)
	for _, wire := range []bool{false, true} {
		for _, materialized := range []bool{false, true} {
			t.Run(fmt.Sprintf("wire=%v/materialized=%v", wire, materialized), func(t *testing.T) {
				ctx := t.Context()
				client := Ydb_Query_V1.NewQueryServiceClient(conn)
				if wire {
					client = WireQueryClient(client)
				}
				stream, err := execute(ctx, "session", client, "SELECT concurrent row",
					options.ExecuteSettings(options.WithResponsePartPrefetch(2)), options.ResultSetsTypeOrdered)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, stream.Close(ctx)) })
				var result publicquery.Result = stream
				if materialized {
					result, err = resultToMaterializedResult(ctx, stream)
					require.NoError(t, err)
				}
				rs, err := result.NextResultSet(ctx)
				require.NoError(t, err)
				row, err := rs.NextRow(ctx)
				require.NoError(t, err)
				_, err = rs.NextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
				require.NoError(t, result.Close(ctx))

				requireConcurrentRowScans(t, row, server.columns)
			})
		}
	}
}

func TestWireRowSupportsConcurrentScans(t *testing.T) {
	server := concurrentWireResultServer()
	frame, err := proto.Marshal(server.rows[0])
	require.NoError(t, err)
	for _, prefixCached := range []bool{false, true} {
		t.Run(fmt.Sprintf("prefixCached=%v", prefixCached), func(t *testing.T) {
			part := &wirePart{frame: frame, rows: []rowSpan{{end: uint32(len(frame))}}}
			row := part.row(0, server.columns)
			if prefixCached {
				var first uint64
				require.NoError(t, row.ScanNamed(scanner.NamedRef("column_0", &first)))
				require.Equal(t, uint64(1), first)
			}
			requireConcurrentRowScans(t, row, server.columns)
		})
	}
}

func concurrentWireResultServer() *optionalWireResultServer {
	const width = 64
	server := &optionalWireResultServer{columns: make([]*Ydb.Column, width)}
	items := make([]*Ydb.Value, width)
	for column := range server.columns {
		server.columns[column] = &Ydb.Column{
			Name: fmt.Sprintf("column_%d", column), Type: wirePrimitive(Ydb.Type_UINT64),
		}
		items[column] = &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: uint64(column + 1)}}
	}
	server.rows = []*Ydb.Value{{Items: items}}

	return server
}

func requireConcurrentRowScans(t *testing.T, row publicquery.Row, columns []*Ydb.Column) {
	t.Helper()
	const workers = 16
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	start := make(chan struct{})
	errors := make(chan error, workers)
	for worker := range workers {
		go func() {
			defer done.Done()
			got := make([]uint64, len(columns))
			named := make([]scanner.NamedDestination, len(columns))
			refs := make([]any, len(columns))
			for column := range got {
				named[len(got)-column-1] = scanner.NamedRef(columns[column].GetName(), &got[column])
				refs[column] = &got[column]
			}
			ready.Done()
			<-start
			for range 20 {
				var err error
				switch worker % 4 {
				case 0:
					err = row.ScanNamed(named...)
				case 1:
					for column, item := range row.Values() {
						if err = value.CastTo(item, &got[column]); err != nil {
							break
						}
					}
				case 2:
					err = row.Scan(refs...)
				case 3:
					var dst struct {
						First uint64 `sql:"column_0"`
						Last  uint64 `sql:"column_63"`
					}
					err = row.ScanStruct(&dst, scanner.WithAllowMissingFieldsInStruct())
					if err == nil && (dst.First != 1 || dst.Last != uint64(len(columns))) {
						err = fmt.Errorf("struct values = (%d, %d), want (1, %d)", dst.First, dst.Last, len(columns))
					}
				}
				if err != nil {
					errors <- err

					return
				}
				if worker%4 == 3 {
					continue
				}
				for column, item := range got {
					if want := uint64(column + 1); item != want {
						errors <- fmt.Errorf("column %d = %d, want %d", column, item, want)

						return
					}
				}
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}
