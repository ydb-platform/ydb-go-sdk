package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

var (
	serializableReadWrite = &Ydb_Query.TransactionSettings_SerializableReadWrite{
		SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
	}
	staleReadOnly = &Ydb_Query.TransactionSettings_StaleReadOnly{
		StaleReadOnly: &Ydb_Query.StaleModeSettings{},
	}
	snapshotReadOnly = &Ydb_Query.TransactionSettings_SnapshotReadOnly{
		SnapshotReadOnly: &Ydb_Query.SnapshotModeSettings{},
	}
)

// Transaction control options
type (
	TxOption            func(*TransactionSettings)
	TransactionSettings struct {
		settings Ydb_Query.TransactionSettings
	}
)

func (txSettings *TransactionSettings) Desc() *Ydb_Query.TransactionSettings {
	return &txSettings.settings
}

// TxSettings returns transaction settings
func TxSettings(opts ...TxOption) *TransactionSettings {
	s := new(TransactionSettings)
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// BeginTx returns begin transaction control option
func BeginTx(opts ...TxOption) TxControlOption {
	return func(d *txControlDesc) {
		s := TxSettings(opts...)
		d.TxSelector = &Ydb_Query.TransactionControl_BeginTx{
			BeginTx: &s.settings,
		}
	}
}

func WithTx(t TransactionIdentifier) TxControlOption {
	return func(d *txControlDesc) {
		d.TxSelector = &Ydb_Query.TransactionControl_TxId{
			TxId: t.ID(),
		}
	}
}

func WithTxID(txID string) TxControlOption {
	return func(d *txControlDesc) {
		d.TxSelector = &Ydb_Query.TransactionControl_TxId{
			TxId: txID,
		}
	}
}

// CommitTx returns commit transaction control option
func CommitTx() TxControlOption {
	return func(d *txControlDesc) {
		d.CommitTx = true
	}
}

func WithDefaultTxMode() TxOption {
	return WithSerializableReadWrite()
}

func WithSerializableReadWrite() TxOption {
	return func(d *TransactionSettings) {
		d.settings.TxMode = serializableReadWrite
	}
}

func WithSnapshotReadOnly() TxOption {
	return func(d *TransactionSettings) {
		d.settings.TxMode = snapshotReadOnly
	}
}

func WithStaleReadOnly() TxOption {
	return func(d *TransactionSettings) {
		d.settings.TxMode = staleReadOnly
	}
}

func WithOnlineReadOnly(opts ...TxOnlineReadOnlyOption) TxOption {
	return func(d *TransactionSettings) {
		var ro txOnlineReadOnly
		for _, opt := range opts {
			if opt != nil {
				opt(&ro)
			}
		}
		d.settings.TxMode = &Ydb_Query.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: (*Ydb_Query.OnlineModeSettings)(&ro),
		}
	}
}

type (
	txOnlineReadOnly       Ydb_Query.OnlineModeSettings
	TxOnlineReadOnlyOption func(*txOnlineReadOnly)
)

func WithInconsistentReads() TxOnlineReadOnlyOption {
	return func(d *txOnlineReadOnly) {
		d.AllowInconsistentReads = true
	}
}

type (
	txControlDesc   Ydb_Query.TransactionControl
	TxControlOption func(*txControlDesc)
)

type TransactionControl struct {
	desc Ydb_Query.TransactionControl
}

func (t *TransactionControl) Desc() *Ydb_Query.TransactionControl {
	if t == nil {
		return nil
	}
	return &t.desc
}

// TxControl makes transaction control from given options
func TxControl(opts ...TxControlOption) *TransactionControl {
	c := new(TransactionControl)
	for _, opt := range opts {
		if opt != nil {
			opt((*txControlDesc)(&c.desc))
		}
	}
	return c
}

// DefaultTxControl returns default transaction control with serializable read-write isolation mode and auto-commit
func DefaultTxControl() *TransactionControl {
	return TxControl(
		BeginTx(WithSerializableReadWrite()),
		CommitTx(),
	)
}

// SerializableReadWriteTxControl returns transaction control with serializable read-write isolation mode
func SerializableReadWriteTxControl(opts ...TxControlOption) *TransactionControl {
	return TxControl(
		append([]TxControlOption{
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
