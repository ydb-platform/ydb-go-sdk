package query

import (
	"context"
	"io"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestTransactionIDAfterInitialization(t *testing.T) {
	for _, method := range []struct {
		name string
		run  func(context.Context, *Transaction) error
	}{
		{
			name: "Exec",
			run: func(ctx context.Context, tx *Transaction) error {
				return tx.Exec(ctx, "SELECT 1")
			},
		},
		{
			name: "Query",
			run: func(ctx context.Context, tx *Transaction) error {
				r, err := tx.Query(ctx, "SELECT 1")
				if err != nil {
					return err
				}

				return r.Close(ctx)
			},
		},
		{
			name: "QueryRow",
			run: func(ctx context.Context, tx *Transaction) error {
				_, err := tx.QueryRow(ctx, "SELECT 1")

				return err
			},
		},
		{
			name: "QueryResultSet",
			run: func(ctx context.Context, tx *Transaction) error {
				r, err := tx.QueryResultSet(ctx, "SELECT 1")
				if err != nil {
					return err
				}

				return r.Close(ctx)
			},
		},
	} {
		t.Run(method.name, func(t *testing.T) {
			for _, initialization := range []string{"Query", "UnLazy", "Begin"} {
				t.Run(initialization, func(t *testing.T) {
					const txID = "transaction-id"

					e := fixenv.New(t)
					ctx := t.Context()
					s := SessionOverGrpcMock(e)
					tx := &Transaction{s: s, txSettings: query.TxSettings(query.WithSerializableReadWrite())}

					switch initialization {
					case "Query":
						expectTransactionIDQuery(e, baseTx.LazyTxID, txID)
						require.NoError(t, method.run(ctx, tx))
					case "UnLazy", "Begin":
						QueryGrpcMock(e).EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(
							&Ydb_Query.BeginTransactionResponse{
								Status: Ydb.StatusIds_SUCCESS,
								TxMeta: &Ydb_Query.TransactionMeta{Id: txID},
							}, nil,
						)
						if initialization == "UnLazy" {
							require.NoError(t, tx.UnLazy(ctx))
						} else {
							startedTx, err := s.Begin(baseTx.WithLazyTx(ctx, false), tx.txSettings)
							require.NoError(t, err)
							tx = startedTx.(*Transaction)
						}
					}

					require.Equal(t, txID, tx.ID())

					// Topic writers read the ID in the background after the first Write.
					stop, done := make(chan struct{}), make(chan struct{})
					t.Cleanup(func() {
						close(stop)
						<-done
					})
					go func() {
						defer close(done)
						for {
							select {
							case <-stop:
								return
							default:
								if got := tx.ID(); got != txID {
									t.Errorf("transaction ID changed: %q", got)

									return
								}
							}
						}
					}()

					for range 3 {
						require.NoError(t, tx.UnLazy(ctx))
						expectTransactionIDQuery(e, txID, txID)
						require.NoError(t, method.run(ctx, tx))
					}
				})
			}
		})
	}
}

func expectTransactionIDQuery(e fixenv.Env, requestTxID, responseTxID string) {
	stream := newExecuteQueryStreamMock(MockController(e))
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		TxMeta: &Ydb_Query.TransactionMeta{Id: responseTxID},
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{{
				Name: "value",
				Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
			}},
			Rows: []*Ydb.Value{{Items: []*Ydb.Value{{Value: &Ydb.Value_Int32Value{Int32Value: 1}}}}},
		},
	}, nil)
	// Metadata may also arrive in a later part of the same result stream.
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		TxMeta: &Ydb_Query.TransactionMeta{Id: responseTxID},
	}, nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)

	QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Cond(
		func(value any) bool {
			request := value.(*Ydb_Query.ExecuteQueryRequest)
			if requestTxID == baseTx.LazyTxID {
				return request.GetTxControl().GetBeginTx() != nil
			}

			return request.GetTxControl().GetTxId() == requestTxID
		},
	)).Return(stream, nil)
}
