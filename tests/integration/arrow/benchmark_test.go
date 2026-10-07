//go:build integration && (darwin || linux)

package witharrow

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/wirevalue"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func BenchmarkFormats(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close(ctx)
	if err := db.Query().Exec(ctx, `CREATE TABLE query_arrow_benchmark (
  id Uint64 NOT NULL, score Int32, active Bool, amount Double, name Utf8, payload String, PRIMARY KEY(id)
);`); err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := db.Query().Exec(ctx, "DROP TABLE query_arrow_benchmark;"); err != nil {
			b.Error(err)
		}
	}()
	fixture := make([]types.Value, 10000)
	for i := range fixture {
		score := types.OptionalValue(types.Int32Value(int32(i % 1000)))
		name := types.OptionalValue(types.TextValue(fmt.Sprintf("customer-%05d", i)))
		if i%10 == 0 {
			score = types.NullValue(types.TypeInt32)
			name = types.NullValue(types.TypeText)
		}
		fixture[i] = types.StructValue(
			types.StructFieldValue("id", types.Uint64Value(uint64(i))),
			types.StructFieldValue("score", score),
			types.StructFieldValue("active", types.OptionalValue(types.BoolValue(i%2 == 0))),
			types.StructFieldValue("amount", types.OptionalValue(types.DoubleValue(float64(i)/4))),
			types.StructFieldValue("name", name),
			types.StructFieldValue("payload", types.OptionalValue(types.BytesValue([]byte(strings.Repeat("x", 64))))),
		)
	}
	for i := 0; i < len(fixture); i += 1000 {
		if err := db.Table().BulkUpsert(ctx, "/local/query_arrow_benchmark",
			table.BulkUpsertDataRows(types.ListValue(fixture[i:i+1000]...))); err != nil {
			b.Fatal(err)
		}
	}
	endpoint, err := url.Parse(connectionString())
	if err != nil {
		b.Fatal(err)
	}
	conn, err := grpc.NewClient(endpoint.Host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	arrowOption := query.WithResultFormatArrow(ipc.NewReader)
	err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		for _, size := range []int{1, 10, 100, 1000, 10000} {
			sql := fmt.Sprintf(
				"SELECT id,score,active,amount,name,payload FROM query_arrow_benchmark WHERE id < %d ORDER BY id;", size,
			)
			count, expected, err := consumeRows(ctx, s, sql, nil)
			if err != nil {
				return err
			}
			if count != size {
				return fmt.Errorf("fixture rows=%d, want %d", count, size)
			}
			b.Logf("rows=%d checksum=%016x", size, expected)
			for _, variant := range []string{"Query", "QueryArrow", "WithResultFormatArrow", "WireValue"} {
				b.Run(fmt.Sprintf("%d/%s", size, variant), func(b *testing.B) {
					run := func() (int, uint64, error) {
						if variant == "QueryArrow" {
							return consumeArrow(ctx, s, sql)
						}
						if variant == "WireValue" {
							return consumeWireValue(ctx, conn, endpoint.Path, s.ID(), sql)
						}
						var opts []query.ExecuteOption
						if variant == "WithResultFormatArrow" {
							opts = append(opts, arrowOption)
						}

						return consumeRows(ctx, s, sql, opts)
					}
					for range 10 {
						n, h, err := run()
						if err != nil || n != size || h != expected {
							b.Fatalf("warmup: rows=%d checksum=%016x err=%v", n, h, err)
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					startCPU := processCPU(b)
					for i := 0; i < b.N; i++ {
						n, h, err := run()
						if err != nil || n != size || h != expected {
							b.Fatalf("rows=%d checksum=%016x err=%v", n, h, err)
						}
					}
					cpu := processCPU(b) - startCPU
					b.StopTimer()
					b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/op")
				})
			}
		}

		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
}

func wireQueryRequest(sessionID, sql string) *Ydb_Query.ExecuteQueryRequest {
	return &Ydb_Query.ExecuteQueryRequest{
		SessionId: sessionID,
		ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
		Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{QueryContent: &Ydb_Query.QueryContent{
			Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
			Text:   sql,
		}},
		ResponsePartLimitBytes: 32 << 10,
	}
}

func consumeWireValue(ctx context.Context, conn *grpc.ClientConn, database, sessionID, sql string) (int, uint64, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "x-ydb-database", database)
	stream, err := Ydb_Query_V1.NewQueryServiceClient(conn).ExecuteQuery(ctx,
		wireQueryRequest(sessionID, sql), grpc.ForceCodecV2(wirevalue.NewCodec()))
	if err != nil {
		return 0, 0, err
	}
	defer stream.CloseSend()
	var columns []*Ydb.Column
	n := 0
	hash := uint64(14695981039346656037)
	var id uint64
	var score *int32
	var active *bool
	var amount *float64
	var name *string
	var payload *[]byte
	dst := []any{&id, &score, &active, &amount, &name, &payload}
	for {
		var part wirevalue.Part
		if err := stream.RecvMsg(&part); err != nil {
			if err == io.EOF {
				return n, hash, nil
			}
			return 0, 0, err
		}
		if part.Meta().GetStatus() != Ydb.StatusIds_SUCCESS {
			return 0, 0, fmt.Errorf("query part status: %v (%v)", part.Meta().GetStatus(), part.Meta().GetIssues())
		}
		if resultSet := part.Meta().GetResultSet(); resultSet != nil {
			if len(resultSet.GetColumns()) != 0 {
				columns = resultSet.GetColumns()
			} else {
				resultSet.Columns = columns
			}
		}
		for i := 0; i < part.RowCount(); i++ {
			if err := part.Row(i).Scan(dst...); err != nil {
				return 0, 0, err
			}
			hash = checksum(hash, id, score, active, amount, name, payload)
			n++
		}
	}
}

func consumeRows(ctx context.Context, s query.Session, sql string, opts []query.ExecuteOption) (int, uint64, error) {
	res, err := s.Query(ctx, sql, append(opts, query.WithResponsePartLimitSizeBytes(32<<10))...)
	if err != nil {
		return 0, 0, err
	}
	defer res.Close(ctx)
	n := 0
	hash := uint64(14695981039346656037)
	var id uint64
	var score *int32
	var active *bool
	var amount *float64
	var name *string
	var payload *[]byte
	dst := []any{&id, &score, &active, &amount, &name, &payload}
	for rs, err := range res.ResultSets(ctx) {
		if err != nil {
			return 0, 0, err
		}
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				return 0, 0, err
			}
			if err := row.Scan(dst...); err != nil {
				return 0, 0, err
			}
			hash = checksum(hash, id, score, active, amount, name, payload)
			n++
		}
	}

	return n, hash, res.Close(ctx)
}

func consumeArrow(ctx context.Context, s query.Session, sql string) (int, uint64, error) {
	res, err := s.QueryArrow(ctx, sql, query.WithResponsePartLimitSizeBytes(32<<10))
	if err != nil {
		return 0, 0, err
	}
	defer res.Close(ctx)
	n := 0
	hash := uint64(14695981039346656037)
	for part, err := range res.Parts(ctx) {
		if err != nil {
			return 0, 0, err
		}
		reader, err := ipc.NewReader(part)
		if err != nil {
			return 0, 0, err
		}
		for reader.Next() {
			batch := reader.RecordBatch()
			ids := batch.Column(0).(*array.Uint64)
			scores := batch.Column(1).(*array.Int32)
			actives := batch.Column(2).(*array.Uint8)
			amounts := batch.Column(3).(*array.Float64)
			names := batch.Column(4).(*array.String)
			payloads := batch.Column(5).(*array.Binary)
			for i := 0; i < int(batch.NumRows()); i++ {
				var score *int32
				var active *bool
				var amount *float64
				var name *string
				var payload *[]byte
				if !scores.IsNull(i) {
					v := scores.Value(i)
					score = &v
				}
				if !actives.IsNull(i) {
					v := actives.Value(i) != 0
					active = &v
				}
				if !amounts.IsNull(i) {
					v := amounts.Value(i)
					amount = &v
				}
				if !names.IsNull(i) {
					v := names.Value(i)
					name = &v
				}
				if !payloads.IsNull(i) {
					v := payloads.Value(i)
					payload = &v
				}
				hash = checksum(hash, ids.Value(i), score, active, amount, name, payload)
				n++
			}
		}
		err = reader.Err()
		reader.Release()
		if err != nil {
			return 0, 0, err
		}
	}

	return n, hash, res.Close(ctx)
}

func checksum(h, id uint64, score *int32, active *bool, amount *float64, name *string, payload *[]byte) uint64 {
	mix := func(v uint64) { h = (h ^ v) * 1099511628211 }
	mix(id)
	if score == nil {
		mix(0)
	} else {
		mix(1)
		mix(uint64(*score))
	}
	if active == nil {
		mix(0)
	} else {
		mix(1)
		if *active {
			mix(1)
		} else {
			mix(0)
		}
	}
	if amount == nil {
		mix(0)
	} else {
		mix(1)
		mix(math.Float64bits(*amount))
	}
	if name == nil {
		mix(0)
	} else {
		mix(1)
		for _, c := range []byte(*name) {
			mix(uint64(c))
		}
	}
	if payload == nil {
		mix(0)
	} else {
		mix(1)
		for _, c := range *payload {
			mix(uint64(c))
		}
	}

	return h
}

func processCPU(b *testing.B) time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}

	return time.Duration(usage.Utime.Sec+usage.Stime.Sec)*time.Second +
		time.Duration(usage.Utime.Usec+usage.Stime.Usec)*time.Microsecond
}
