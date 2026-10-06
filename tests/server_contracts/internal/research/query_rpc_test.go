package research_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/tests/server_contracts/internal/grpcclient"
)

type queryTransaction struct {
	client              Ydb_Query_V1.QueryServiceClient
	id                  string
	sessionID           string
	attachCancel        context.CancelFunc
	attachDone          chan struct{}
	attachErr           error
	closeOnce           sync.Once
	closeErr            error
	finished            bool
	completionOperation string
	completionErr       error
}

func beginQueryTransaction(ctx context.Context, conn grpc.ClientConnInterface) (_ *queryTransaction, resultErr error) {
	client := Ydb_Query_V1.NewQueryServiceClient(conn)
	created, err := client.CreateSession(ctx, &Ydb_Query.CreateSessionRequest{})
	if err != nil {
		return nil, err
	}
	if err := grpcclient.CheckStatus("CreateSession", created.GetStatus(), created.GetIssues()); err != nil {
		return nil, err
	}
	if created.GetSessionId() == "" {
		return nil, errors.New("CreateSession returned no session ID")
	}
	tx := &queryTransaction{client: client, sessionID: created.GetSessionId()}
	defer func() {
		if resultErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, tx.Close(cleanupCtx))
		}
	}()
	attachCtx, cancel := context.WithCancel(ctx)
	tx.attachCancel = cancel
	stream, err := client.AttachSession(attachCtx, &Ydb_Query.AttachSessionRequest{SessionId: tx.sessionID})
	if err != nil {
		return nil, err
	}
	attached, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if err := grpcclient.CheckStatus("AttachSession", attached.GetStatus(), attached.GetIssues()); err != nil {
		return nil, err
	}
	if attached.GetSessionShutdown() != nil || attached.GetNodeShutdown() != nil {
		return nil, fmt.Errorf("AttachSession returned a shutdown hint: %v", attached)
	}
	tx.attachDone = make(chan struct{})
	// Keep receiving and recording the raw attachment stream. A server termination
	// never starts another session or replays an experiment.
	go func() {
		defer close(tx.attachDone)
		for {
			state, err := stream.Recv()
			if err != nil {
				tx.attachErr = err

				return
			}
			if err := grpcclient.CheckStatus("AttachSession", state.GetStatus(), state.GetIssues()); err != nil {
				tx.attachErr = err

				return
			}
			if state.GetSessionShutdown() != nil || state.GetNodeShutdown() != nil {
				tx.attachErr = fmt.Errorf("AttachSession returned a shutdown hint: %v", state)

				return
			}
		}
	}()
	begun, err := client.BeginTransaction(ctx, &Ydb_Query.BeginTransactionRequest{
		SessionId: tx.sessionID,
		TxSettings: &Ydb_Query.TransactionSettings{
			TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
				SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if err := grpcclient.CheckStatus("BeginTransaction", begun.GetStatus(), begun.GetIssues()); err != nil {
		return nil, err
	}
	tx.id = begun.GetTxMeta().GetId()
	if tx.id == "" {
		return nil, errors.New("BeginTransaction returned no transaction ID")
	}

	return tx, nil
}

func (t *queryTransaction) Commit(ctx context.Context) error {
	response, err := t.client.CommitTransaction(ctx, &Ydb_Query.CommitTransactionRequest{
		SessionId: t.sessionID, TxId: t.id,
	})
	if err != nil {
		return err
	}

	return grpcclient.CheckStatus("CommitTransaction", response.GetStatus(), response.GetIssues())
}

func (t *queryTransaction) Rollback(ctx context.Context) error {
	response, err := t.client.RollbackTransaction(ctx, &Ydb_Query.RollbackTransactionRequest{
		SessionId: t.sessionID, TxId: t.id,
	})
	if err != nil {
		return err
	}

	return grpcclient.CheckStatus("RollbackTransaction", response.GetStatus(), response.GetIssues())
}

func (t *queryTransaction) Close(ctx context.Context) error {
	t.closeOnce.Do(func() {
		if t.attachDone != nil {
			select {
			case <-t.attachDone:
				t.closeErr = t.attachErr
			default:
			}
		}
		response, err := t.client.DeleteSession(ctx, &Ydb_Query.DeleteSessionRequest{SessionId: t.sessionID})
		if err == nil {
			err = grpcclient.CheckStatus("DeleteSession", response.GetStatus(), response.GetIssues())
		}
		t.closeErr = errors.Join(t.closeErr, err)
		if t.attachCancel != nil {
			t.attachCancel()
		}
		if t.attachDone != nil {
			select {
			case <-t.attachDone:
			case <-ctx.Done():
				t.closeErr = errors.Join(t.closeErr, ctx.Err())
			}
		}
	})

	return t.closeErr
}
