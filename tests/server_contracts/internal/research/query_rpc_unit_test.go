package research_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/tests/server_contracts/internal/grpcclient"
)

func TestQueryTransactionUsesOneSessionWithoutReplay(t *testing.T) {
	for _, outcome := range []string{"commit", "rollback", "aborted", "transport", "begin rejected", "attach rejected"} {
		t.Run(outcome, func(t *testing.T) {
			service := &queryRPCServer{outcome: outcome}
			conn := startQueryRPCServer(t, service)
			tx, err := beginQueryTransaction(t.Context(), conn)
			want := []string{"CreateSession", "AttachSession"}
			if outcome != "attach rejected" {
				want = append(want, "BeginTransaction")
			}
			if outcome == "begin rejected" || outcome == "attach rejected" {
				var serverErr *grpcclient.StatusError
				if tx != nil || !errors.As(err, &serverErr) || serverErr.Status != Ydb.StatusIds_OVERLOADED {
					t.Fatalf("initialization must preserve rejection without retrying: tx=%v err=%v", tx, err)
				}
				assertQueryRPCMethods(t, service, append(want, "DeleteSession"))

				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tx.id != "tx" || tx.sessionID != "session" {
				t.Fatalf("unexpected identity: %v", tx)
			}
			world := &researchWorld{research: &streamWriteResearch{namedTransactions: map[string]*queryTransaction{"A": tx}}}
			ctx := context.WithValue(t.Context(), worldContextKey{}, world)
			operation, expected := "Commit", "SUCCESS"
			if outcome == "rollback" {
				operation = "Rollback"
				err = stepNamedQueryTransactionRollback(ctx, "A")
			} else {
				err = stepNamedQueryTransactionCommit(ctx, "A")
			}
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "aborted" {
				expected = "ABORTED"
				var serverErr *grpcclient.StatusError
				if !errors.As(tx.completionErr, &serverErr) || len(serverErr.Issues) != 1 ||
					!proto.Equal(serverErr.Issues[0], queryIssueForTest()) {
					t.Fatalf("server issue tree was lost: %v", tx.completionErr)
				}
			}
			if outcome == "transport" {
				if status.Code(tx.completionErr) != codes.Unavailable ||
					contractTransactionResult(ctx, operation, "A", "SUCCESS") == nil {
					t.Fatalf("transport failure was hidden: %v", tx.completionErr)
				}
			} else if err := contractTransactionResult(ctx, operation, "A", expected); err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			want = append(want, operation+"Transaction")
			want = append(want, "DeleteSession")
			assertQueryRPCMethods(t, service, want)
		})
	}
}

type queryRPCServer struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer

	outcome   string
	mu        sync.Mutex
	methods   []string
	sessionID string
}

func assertQueryRPCMethods(t *testing.T, service *queryRPCServer, want []string) {
	t.Helper()
	service.mu.Lock()
	defer service.mu.Unlock()
	if !reflect.DeepEqual(service.methods, want) || service.sessionID != "session" {
		t.Fatalf("session lifecycle: methods=%v session=%q; want %v", service.methods, service.sessionID, want)
	}
}

func startQueryRPCServer(t *testing.T, service *queryRPCServer) *grpcclient.Conn {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	Ydb_Query_V1.RegisterQueryServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpcclient.Open("grpc://" + listener.Addr().String() + "/local")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func (s *queryRPCServer) record(method, sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.methods = append(s.methods, method)
	s.sessionID = sessionID
}

func (s *queryRPCServer) CreateSession(
	context.Context, *Ydb_Query.CreateSessionRequest,
) (*Ydb_Query.CreateSessionResponse, error) {
	s.record("CreateSession", "")

	return &Ydb_Query.CreateSessionResponse{Status: Ydb.StatusIds_SUCCESS, SessionId: "session"}, nil
}

func (s *queryRPCServer) AttachSession(
	request *Ydb_Query.AttachSessionRequest, stream Ydb_Query_V1.QueryService_AttachSessionServer,
) error {
	s.record("AttachSession", request.GetSessionId())
	code := Ydb.StatusIds_SUCCESS
	if s.outcome == "attach rejected" {
		code = Ydb.StatusIds_OVERLOADED
	}
	if err := stream.Send(&Ydb_Query.SessionState{Status: code}); err != nil {
		return err
	}
	<-stream.Context().Done()

	return stream.Context().Err()
}

func (s *queryRPCServer) BeginTransaction(
	_ context.Context, request *Ydb_Query.BeginTransactionRequest,
) (*Ydb_Query.BeginTransactionResponse, error) {
	s.record("BeginTransaction", request.GetSessionId())
	if request.GetTxSettings().GetSerializableReadWrite() == nil {
		return nil, status.Error(codes.InvalidArgument, "expected serializable read-write")
	}
	code := Ydb.StatusIds_SUCCESS
	if s.outcome == "begin rejected" {
		code = Ydb.StatusIds_OVERLOADED
	}

	return &Ydb_Query.BeginTransactionResponse{Status: code, TxMeta: &Ydb_Query.TransactionMeta{Id: "tx"}}, nil
}

func (s *queryRPCServer) CommitTransaction(
	_ context.Context, request *Ydb_Query.CommitTransactionRequest,
) (*Ydb_Query.CommitTransactionResponse, error) {
	s.record("CommitTransaction", request.GetSessionId())
	if request.GetTxId() != "tx" {
		return nil, status.Error(codes.InvalidArgument, "wrong transaction")
	}
	if s.outcome == "transport" {
		return nil, status.Error(codes.Unavailable, "injected transport failure")
	}
	if s.outcome == "aborted" {
		return &Ydb_Query.CommitTransactionResponse{
			Status: Ydb.StatusIds_ABORTED, Issues: []*Ydb_Issue.IssueMessage{queryIssueForTest()},
		}, nil
	}

	return &Ydb_Query.CommitTransactionResponse{Status: Ydb.StatusIds_SUCCESS}, nil
}

func (s *queryRPCServer) RollbackTransaction(
	_ context.Context, request *Ydb_Query.RollbackTransactionRequest,
) (*Ydb_Query.RollbackTransactionResponse, error) {
	s.record("RollbackTransaction", request.GetSessionId())
	if request.GetTxId() != "tx" {
		return nil, status.Error(codes.InvalidArgument, "wrong transaction")
	}

	return &Ydb_Query.RollbackTransactionResponse{Status: Ydb.StatusIds_SUCCESS}, nil
}

func (s *queryRPCServer) DeleteSession(
	_ context.Context, request *Ydb_Query.DeleteSessionRequest,
) (*Ydb_Query.DeleteSessionResponse, error) {
	s.record("DeleteSession", request.GetSessionId())

	return &Ydb_Query.DeleteSessionResponse{Status: Ydb.StatusIds_SUCCESS}, nil
}

func queryIssueForTest() *Ydb_Issue.IssueMessage {
	return &Ydb_Issue.IssueMessage{
		IssueCode: 1, Message: "outer", Issues: []*Ydb_Issue.IssueMessage{{IssueCode: 2, Message: "inner"}},
	}
}
