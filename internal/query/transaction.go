package query

import (
	"context"
	"io"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ query.Transaction = (*Transaction)(nil)
	_ tx.Identifier     = (*Transaction)(nil)
)

type Transaction struct {
	tx.Identifier

	s               *Session
	rollbackStarted atomic.Bool

	m                 sync.Mutex
	onCompletedCalled bool
	onCompleted       []tx.OnTransactionCompletedFunc
}

func (tx *Transaction) SessionID() string {
	return tx.s.ID()
}

func newTransaction(id string, s *Session) *Transaction {
	return &Transaction{
		Identifier: tx.ID(id),
		s:          s,
	}
}

func (tx *Transaction) Exec(ctx context.Context, q string, opts ...options.TxExecOption) (
	finalErr error,
) {
	if tx.rollbackStarted.Load() {
		return xerrors.WithStackTrace(ErrTransactionRollingBack)
	}

	onDone := trace.QueryOnTxExec(tx.s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).Exec"), tx.s, tx, q)
	defer func() {
		onDone(finalErr)
	}()

	executeSettings := options.TxExecuteSettings(tx.ID(), opts...).ExecuteSettings

	var resultOpts []resultOption
	if executeSettings.TxControl().IsTxCommit() {
		// notification about complete transaction must be sended for any error or for successfully read all result if
		// it was execution with commit flag
		resultOpts = append(resultOpts,
			onNextPartErr(func(err error) {
				tx.notifyOnCompleted(xerrors.HideEOF(err))
			}),
		)
	}
	_, r, err := execute(ctx, tx.s, tx.s.grpcClient, q, executeSettings, resultOpts...)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	for {
		_, err = r.NextResultSet(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				return nil
			}

			return xerrors.WithStackTrace(err)
		}
	}
}

func (tx *Transaction) Query(ctx context.Context, q string, opts ...options.TxQueryOption) (
	_ query.Result, finalErr error,
) {
	if tx.rollbackStarted.Load() {
		return nil, xerrors.WithStackTrace(ErrTransactionRollingBack)
	}

	onDone := trace.QueryOnTxQuery(tx.s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).Query"), tx.s, tx, q)
	defer func() {
		onDone(finalErr)
	}()

	executeSettings := options.TxExecuteSettings(tx.ID(), opts...).ExecuteSettings

	var resultOpts []resultOption
	if executeSettings.TxControl().IsTxCommit() {
		// notification about complete transaction must be sended for any error or for successfully read all result if
		// it was execution with commit flag
		resultOpts = append(resultOpts,
			onNextPartErr(func(err error) {
				tx.notifyOnCompleted(xerrors.HideEOF(err))
			}),
		)
	}
	_, r, err := execute(ctx, tx.s, tx.s.grpcClient, q, executeSettings, resultOpts...)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func commitTx(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID, txID string) error {
	_, err := client.CommitTransaction(ctx, &Ydb_Query.CommitTransactionRequest{
		SessionId: sessionID,
		TxId:      txID,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) CommitTx(ctx context.Context) (err error) {
	if tx.rollbackStarted.Load() {
		return xerrors.WithStackTrace(ErrTransactionRollingBack)
	}

	defer func() {
		tx.notifyOnCompleted(err)
	}()

	err = commitTx(ctx, tx.s.grpcClient, tx.s.id, tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func rollback(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID, txID string) error {
	_, err := client.RollbackTransaction(ctx, &Ydb_Query.RollbackTransactionRequest{
		SessionId: sessionID,
		TxId:      txID,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) Rollback(ctx context.Context) error {
	// set flag for starting rollback and call notifications
	// allow to rollback transaction many times - for handle retries on errors
	if tx.rollbackStarted.CompareAndSwap(false, true) {
		tx.notifyOnCompleted(xerrors.WithStackTrace(ErrTransactionRollingBack))
	}

	//nolint:godox
	// ToDo save local marker for deny any additional requests to the transaction?

	err := rollback(ctx, tx.s.grpcClient, tx.s.id, tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) OnCompleted(f tx.OnTransactionCompletedFunc) {
	tx.m.Lock()
	defer tx.m.Unlock()

	tx.onCompleted = append(tx.onCompleted, f)
}

func (tx *Transaction) notifyOnCompleted(err error) {
	tx.m.Lock()
	notifyCalled := tx.onCompletedCalled
	tx.onCompletedCalled = true
	onCompletedFunctions := slices.Clone(tx.onCompleted)
	tx.m.Unlock()

	if notifyCalled {
		return
	}

	for _, f := range onCompletedFunctions {
		f(err)
	}
}
