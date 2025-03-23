package tx

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
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
	onlineReadOnlyAllowInconsistentReads = &Ydb_Query.TransactionSettings_OnlineReadOnly{
		OnlineReadOnly: &Ydb_Query.OnlineModeSettings{AllowInconsistentReads: true},
	}
	onlineReadOnlyForbidInconsistentReads = &Ydb_Query.TransactionSettings_OnlineReadOnly{
		OnlineReadOnly: &Ydb_Query.OnlineModeSettings{AllowInconsistentReads: false},
	}
)

// Transaction settings options
type (
	Option interface {
		ApplyTxSettingsOption(a *allocator.Allocator, txSettings *Ydb_Query.TransactionSettings)
	}
	Settings []Option
)

func (opts Settings) applyTxSelector(a *allocator.Allocator, txControl *Ydb_Query.TransactionControl) {
	beginTx := a.QueryTransactionControlBeginTx()
	beginTx.BeginTx = a.QueryTransactionSettings()
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyTxSettingsOption(a, beginTx.BeginTx)
		}
	}
	txControl.TxSelector = beginTx
}

func (opts Settings) ToYDB(a *allocator.Allocator) *Ydb_Query.TransactionSettings {
	txSettings := a.QueryTransactionSettings()
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyTxSettingsOption(a, txSettings)
		}
	}

	return txSettings
}

// NewSettings returns transaction settings
func NewSettings(opts ...Option) Settings {
	return opts
}

func WithDefaultTxMode() Option {
	return WithSerializableReadWrite()
}

var _ Option = serializableReadWriteTxSettingsOption{}

type serializableReadWriteTxSettingsOption struct{}

func (o serializableReadWriteTxSettingsOption) ApplyTxSettingsOption(
	a *allocator.Allocator, txSettings *Ydb_Query.TransactionSettings,
) {
	txSettings.TxMode = serializableReadWrite
}

func WithSerializableReadWrite() Option {
	return serializableReadWriteTxSettingsOption{}
}

var _ Option = snapshotReadOnlyTxSettingsOption{}

type snapshotReadOnlyTxSettingsOption struct{}

func (snapshotReadOnlyTxSettingsOption) ApplyTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	settings.TxMode = snapshotReadOnly
}

func WithSnapshotReadOnly() Option {
	return snapshotReadOnlyTxSettingsOption{}
}

var _ Option = staleReadOnlySettingsOption{}

type staleReadOnlySettingsOption struct{}

func (staleReadOnlySettingsOption) ApplyTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	settings.TxMode = staleReadOnly
}

func WithStaleReadOnly() Option {
	return staleReadOnlySettingsOption{}
}

type (
	onlineReadOnly       bool
	OnlineReadOnlyOption interface {
		applyTxOnlineReadOnlyOption(opt *onlineReadOnly)
	}
)

var _ OnlineReadOnlyOption = inconsistentReadsTxOnlineReadOnlyOption{}

type inconsistentReadsTxOnlineReadOnlyOption struct{}

func (i inconsistentReadsTxOnlineReadOnlyOption) applyTxOnlineReadOnlyOption(b *onlineReadOnly) {
	*b = true
}

func WithInconsistentReads() OnlineReadOnlyOption {
	return inconsistentReadsTxOnlineReadOnlyOption{}
}

var _ Option = onlineReadOnlySettingsOption{}

type onlineReadOnlySettingsOption []OnlineReadOnlyOption

func (opts onlineReadOnlySettingsOption) ApplyTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	var ro onlineReadOnly
	for _, opt := range opts {
		if opt != nil {
			opt.applyTxOnlineReadOnlyOption(&ro)
		}
	}
	if ro {
		settings.TxMode = onlineReadOnlyAllowInconsistentReads
	} else {
		settings.TxMode = onlineReadOnlyForbidInconsistentReads
	}
}

func WithOnlineReadOnly(opts ...OnlineReadOnlyOption) onlineReadOnlySettingsOption {
	return opts
}
