package table

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestTxSkipRollbackForCommitted(t *testing.T) {
	var (
		begin    = 0
		commit   = 0
		rollback = 0
	)
	b := StubBuilder{
		T: t,
		cc: testutil.NewRouter(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					testutil.TableBeginTransaction: func(request interface{}) (proto.Message, error) {
						_, ok := request.(*Ydb_Table.BeginTransactionRequest)
						if !ok {
							t.Fatalf("cannot cast request '%T' to *Ydb_Table.BeginTransactionRequest", request)
						}
						result, err := anypb.New(
							&Ydb_Table.BeginTransactionResult{
								TxMeta: &Ydb_Table.TransactionMeta{
									Id: "",
								},
							},
						)
						if err != nil {
							return nil, err
						}
						begin++
						return &Ydb_Table.BeginTransactionResponse{
							Operation: &Ydb_Operations.Operation{
								Ready:  true,
								Status: Ydb.StatusIds_SUCCESS,
								Result: result,
							},
						}, nil
					},
					testutil.TableCommitTransaction: func(request interface{}) (proto.Message, error) {
						_, ok := request.(*Ydb_Table.CommitTransactionRequest)
						if !ok {
							t.Fatalf("cannot cast request '%T' to *Ydb_Table.CommitTransactionRequest", request)
						}
						result, err := anypb.New(
							&Ydb_Table.CommitTransactionResult{},
						)
						if err != nil {
							return nil, err
						}
						commit++
						return &Ydb_Table.CommitTransactionResponse{
							Operation: &Ydb_Operations.Operation{
								Ready:  true,
								Status: Ydb.StatusIds_SUCCESS,
								Result: result,
							},
						}, nil
					},
					testutil.TableRollbackTransaction: func(request interface{}) (proto.Message, error) {
						_, ok := request.(*Ydb_Table.RollbackTransactionRequest)
						if !ok {
							t.Fatalf("cannot cast request '%T' to *Ydb_Table.RollbackTransactionRequest", request)
						}
						rollback++
						return &Ydb_Table.RollbackTransactionResponse{
							Operation: &Ydb_Operations.Operation{
								Ready:  true,
								Status: Ydb.StatusIds_SUCCESS,
							},
						}, nil
					},
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
				},
			),
		),
	}
	s, err := b.createSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	{
		x, err := s.BeginTransaction(context.Background(), table.TxSettings())
		if err != nil {
			t.Fatal(err)
		}
		if begin != 1 {
			t.Fatalf("unexpected begin: %d", begin)
		}
		_, err = x.CommitTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if commit != 1 {
			t.Fatalf("unexpected commit: %d", begin)
		}
		_, _ = x.CommitTx(context.Background())
		if commit != 1 {
			t.Fatalf("unexpected commit: %d", begin)
		}
		err = x.Rollback(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if rollback != 0 {
			t.Fatalf("unexpected rollback: %d", begin)
		}
	}
	{
		x, err := s.BeginTransaction(context.Background(), table.TxSettings())
		if err != nil {
			t.Fatal(err)
		}
		if begin != 2 {
			t.Fatalf("unexpected begin: %d", begin)
		}
		err = x.Rollback(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if rollback != 1 {
			t.Fatalf("unexpected rollback: %d", begin)
		}
		_, err = x.CommitTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if commit != 2 {
			t.Fatalf("unexpected commit: %d", begin)
		}
	}
}
