package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

var (
	_ interface {
		ToYDB(a *allocator.Allocator) *Ydb_Query.TransactionControl
	} = (*TransactionControl)(nil)
	_ txSelector = (*TransactionSettings)(nil)
)

type (
	txSelector interface {
		applyTxSelector(a *allocator.Allocator, txControl *Ydb_Query.TransactionControl)
	}
	txControlOption interface {
		applyTxControlOption(txControl *TransactionControl)
	}
	TransactionControl struct {
		selector txSelector
		commit   bool
	}
	transactionControlOption struct {
		txControl *TransactionControl
	}
)

func (opt *transactionControlOption) applyExecuteOption(s *executeSettings) {
	s.txControl = opt.txControl
}

func (ctrl *TransactionControl) ToYDB(a *allocator.Allocator) *Ydb_Query.TransactionControl {
	txControl := a.QueryTransactionControl()
	ctrl.selector.applyTxSelector(a, txControl)
	txControl.CommitTx = ctrl.commit
	return txControl
}

var (
	_ txControlOption = beginTxOptions{}
	_ txSelector      = beginTxOptions{}
)

type beginTxOptions []txSettingsOption

func (opts beginTxOptions) applyTxControlOption(txControl *TransactionControl) {
	txControl.selector = opts
}

func (opts beginTxOptions) applyTxSelector(a *allocator.Allocator, txControl *Ydb_Query.TransactionControl) {
	selector := a.QueryTransactionControlBeginTx()
	selector.BeginTx = a.QueryTransactionSettings()
	for _, opt := range opts {
		opt.applyTxSettingsOption(a, selector.BeginTx)
	}
	txControl.TxSelector = selector
}

// BeginTx returns selector transaction control option
func BeginTx(opts ...txSettingsOption) beginTxOptions {
	return opts
}

var (
	_ txControlOption = txIdTxControlOption("")
	_ txSelector      = txIdTxControlOption("")
)

type txIdTxControlOption string

func (id txIdTxControlOption) applyTxControlOption(txControl *TransactionControl) {
	txControl.selector = id
}

func (id txIdTxControlOption) applyTxSelector(a *allocator.Allocator, txControl *Ydb_Query.TransactionControl) {
	selector := a.QueryTransactionControlTxId()
	selector.TxId = string(id)
	txControl.TxSelector = selector
}

func WithTx(t TxIdentifier) txIdTxControlOption {
	return txIdTxControlOption(t.ID())
}

func WithTxID(txID string) txIdTxControlOption {
	return txIdTxControlOption(txID)
}

type commitTxOption struct{}

func (c commitTxOption) applyTxControlOption(txControl *TransactionControl) {
	txControl.commit = true
}

// CommitTx returns commit transaction control option
func CommitTx() txControlOption {
	return commitTxOption{}
}

// TxControl makes transaction control from given options
func TxControl(opts ...txControlOption) *TransactionControl {
	txControl := &TransactionControl{
		selector: BeginTx(WithSerializableReadWrite()),
		commit:   false,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyTxControlOption(txControl)
		}
	}
	return txControl
}

// DefaultTxControl returns default transaction control with serializable read-write isolation mode and auto-commit
func DefaultTxControl() *TransactionControl {
	return TxControl(
		BeginTx(WithSerializableReadWrite()),
		CommitTx(),
	)
}

// SerializableReadWriteTxControl returns transaction control with serializable read-write isolation mode
func SerializableReadWriteTxControl(opts ...txControlOption) *TransactionControl {
	return TxControl(
		append([]txControlOption{
			BeginTx(WithSerializableReadWrite()),
		}, opts...)...,
	)
}

// OnlineReadOnlyTxControl returns online read-only transaction control
func OnlineReadOnlyTxControl(opts ...TxOnlineReadOnlyOption) *TransactionControl {
	return TxControl(
		BeginTx(WithOnlineReadOnly(opts...)),
		CommitTx(), // open transactions not supported for OnlineReadOnly
	)
}

// StaleReadOnlyTxControl returns stale read-only transaction control
func StaleReadOnlyTxControl() *TransactionControl {
	return TxControl(
		BeginTx(WithStaleReadOnly()),
		CommitTx(), // open transactions not supported for StaleReadOnly
	)
}

// SnapshotReadOnlyTxControl returns snapshot read-only transaction control
func SnapshotReadOnlyTxControl() *TransactionControl {
	return TxControl(
		BeginTx(WithSnapshotReadOnly()),
		CommitTx(), // open transactions not supported for StaleReadOnly
	)
}
