package tx

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

var (
	_ interface {
		ToYDB(a *allocator.Allocator) *Ydb_Query.TransactionControl
	} = (*Control)(nil)
	_ Selector = (*Settings)(nil)
)

type (
	Selector interface {
		applyTxSelector(a *allocator.Allocator, txControl *Ydb_Query.TransactionControl)
	}
	ControlOption interface {
		applyTxControlOption(txControl *Control)
	}
	Control struct {
		selector Selector
		commit   bool
	}
	Identifier interface {
		ID() string
		isYdbTx()
	}
)

func (ctrl *Control) ToYDB(a *allocator.Allocator) *Ydb_Query.TransactionControl {
	if ctrl == nil {
		return nil
	}

	txControl := a.QueryTransactionControl()
	ctrl.selector.applyTxSelector(a, txControl)
	txControl.CommitTx = ctrl.commit

	return txControl
}

var (
	_ ControlOption = beginTxOptions{}
	_ Selector      = beginTxOptions{}
)

type beginTxOptions []Option

func (opts beginTxOptions) applyTxControlOption(txControl *Control) {
	txControl.selector = opts
}

func (opts beginTxOptions) applyTxSelector(a *allocator.Allocator, txControl *Ydb_Query.TransactionControl) {
	selector := a.QueryTransactionControlBeginTx()
	selector.BeginTx = a.QueryTransactionSettings()
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyTxSettingsOption(a, selector.BeginTx)
		}
	}
	txControl.TxSelector = selector
}

// BeginTx returns selector transaction control option
func BeginTx(opts ...Option) beginTxOptions {
	return opts
}

var (
	_ ControlOption = txIDTxControlOption("")
	_ Selector      = txIDTxControlOption("")
)

type txIDTxControlOption string

func (id txIDTxControlOption) applyTxControlOption(txControl *Control) {
	txControl.selector = id
}

func (id txIDTxControlOption) applyTxSelector(a *allocator.Allocator, txControl *Ydb_Query.TransactionControl) {
	selector := a.QueryTransactionControlTxID()
	selector.TxId = string(id)
	txControl.TxSelector = selector
}

func WithTx(t Identifier) txIDTxControlOption {
	return txIDTxControlOption(t.ID())
}

func WithTxID(txID string) txIDTxControlOption {
	return txIDTxControlOption(txID)
}

type commitTxOption struct{}

func (c commitTxOption) applyTxControlOption(txControl *Control) {
	txControl.commit = true
}

// CommitTx returns commit transaction control option
func CommitTx() ControlOption {
	return commitTxOption{}
}

// NewControl makes transaction control from given options
func NewControl(opts ...ControlOption) *Control {
	txControl := &Control{
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

func NoTx() *Control {
	return nil
}

// DefaultTxControl returns default transaction control with serializable read-write isolation mode and auto-commit
func DefaultTxControl() *Control {
	return NoTx()
}

// SerializableReadWriteTxControl returns transaction control with serializable read-write isolation mode
func SerializableReadWriteTxControl(opts ...ControlOption) *Control {
	return NewControl(
		append([]ControlOption{
			BeginTx(WithSerializableReadWrite()),
		}, opts...)...,
	)
}

// OnlineReadOnlyTxControl returns online read-only transaction control
func OnlineReadOnlyTxControl(opts ...OnlineReadOnlyOption) *Control {
	return NewControl(
		BeginTx(WithOnlineReadOnly(opts...)),
		CommitTx(), // open transactions not supported for OnlineReadOnly
	)
}

// StaleReadOnlyTxControl returns stale read-only transaction control
func StaleReadOnlyTxControl() *Control {
	return NewControl(
		BeginTx(WithStaleReadOnly()),
		CommitTx(), // open transactions not supported for StaleReadOnly
	)
}

// SnapshotReadOnlyTxControl returns snapshot read-only transaction control
func SnapshotReadOnlyTxControl() *Control {
	return NewControl(
		BeginTx(WithSnapshotReadOnly()),
		CommitTx(), // open transactions not supported for StaleReadOnly
	)
}
