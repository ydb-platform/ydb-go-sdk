package querywire

import (
	"context"
	"database/sql"
	"flag"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestFlagSelectsQueryDecoder(t *testing.T) {
	for _, mode := range []string{"native", "database_sql"} {
		for _, tt := range []struct {
			name string
			args []string
			wire bool
		}{
			{name: "default"},
			{name: "enabled", args: []string{"-query-proto-wire-decoding"}, wire: true},
			{name: "disabled", args: []string{"-query-proto-wire-decoding=false"}},
		} {
			t.Run(mode+"/"+tt.name, func(t *testing.T) {
				fs := flag.NewFlagSet(tt.name, flag.ContinueOnError)
				fs.SetOutput(io.Discard)
				enabled := RegisterFlag(fs)
				if err := fs.Parse(tt.args); err != nil {
					t.Fatal(err)
				}
				listener := bufconn.Listen(1 << 20)
				server := grpc.NewServer()
				Ydb_Query_V1.RegisterQueryServiceServer(server, decoderServer{})
				go func() { _ = server.Serve(listener) }()
				t.Cleanup(server.Stop)
				observer := &decoderObserver{}
				driver, err := ydb.Open(t.Context(), "grpc://bufnet:2136/local",
					ydb.WithAnonymousCredentials(),
					ydb.WithBalancer(balancers.SingleConn()),
					ydb.With(config.WithGrpcOptions(
						grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
							return listener.Dial()
						}),
						grpc.WithChainStreamInterceptor(observer.intercept),
					)),
					Option(*enabled),
				)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := driver.Close(t.Context()); err != nil {
						t.Error(err)
					}
				})
				var got uint64
				if mode == "database_sql" {
					got = readSQLQueryValue(t, driver)
				} else {
					row, err := driver.Query().QueryRow(t.Context(), "SELECT 42")
					if err != nil {
						t.Fatal(err)
					}
					if err := row.Scan(&got); err != nil {
						t.Fatal(err)
					}
				}
				if got != 42 {
					t.Fatalf("scanned value = %d, want 42", got)
				}
				if got := observer.wire.Load() > 0; got != tt.wire {
					t.Fatalf("wire decoder selected = %v, want %v", got, tt.wire)
				}
				if got := observer.standard.Load() > 0; got == tt.wire {
					t.Fatalf("standard decoder selected = %v, want %v", got, !tt.wire)
				}
			})
		}
	}
}

func readSQLQueryValue(t *testing.T, driver *ydb.Driver) uint64 {
	t.Helper()
	connector, err := ydb.Connector(driver, ydb.WithQueryService(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := connector.Close(); err != nil {
			t.Error(err)
		}
	})
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	var got uint64
	if err := db.QueryRowContext(t.Context(), "SELECT 42").Scan(&got); err != nil {
		t.Fatal(err)
	}

	return got
}

type decoderObserver struct {
	wire     atomic.Uint32
	standard atomic.Uint32
}

func (o *decoderObserver) intercept(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	stream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil || method != Ydb_Query_V1.QueryService_ExecuteQuery_FullMethodName {
		return stream, err
	}

	return decoderStream{ClientStream: stream, observer: o}, nil
}

type decoderStream struct {
	grpc.ClientStream

	observer *decoderObserver
}

func (s decoderStream) RecvMsg(message any) error {
	if _, standard := message.(*Ydb_Query.ExecuteQueryResponsePart); standard {
		s.observer.standard.Add(1)
	} else {
		s.observer.wire.Add(1)
	}

	return s.ClientStream.RecvMsg(message)
}

type decoderServer struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer
}

func (decoderServer) CreateSession(context.Context, *Ydb_Query.CreateSessionRequest) (
	*Ydb_Query.CreateSessionResponse, error,
) {
	return &Ydb_Query.CreateSessionResponse{Status: Ydb.StatusIds_SUCCESS, SessionId: "session"}, nil
}

func (decoderServer) DeleteSession(context.Context, *Ydb_Query.DeleteSessionRequest) (
	*Ydb_Query.DeleteSessionResponse, error,
) {
	return &Ydb_Query.DeleteSessionResponse{Status: Ydb.StatusIds_SUCCESS}, nil
}

func (decoderServer) AttachSession(_ *Ydb_Query.AttachSessionRequest,
	stream Ydb_Query_V1.QueryService_AttachSessionServer,
) error {
	if err := stream.Send(&Ydb_Query.SessionState{Status: Ydb.StatusIds_SUCCESS}); err != nil {
		return err
	}
	<-stream.Context().Done()

	return stream.Context().Err()
}

func (decoderServer) ExecuteQuery(_ *Ydb_Query.ExecuteQueryRequest,
	stream Ydb_Query_V1.QueryService_ExecuteQueryServer,
) error {
	return stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{{Name: "id", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}}}},
			Rows: []*Ydb.Value{{Items: []*Ydb.Value{
				{Value: &Ydb.Value_Uint64Value{Uint64Value: 42}},
			}}},
		},
	})
}
