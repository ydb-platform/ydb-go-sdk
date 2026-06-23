package tx

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

// Transaction settings options
type (
	SettingsOption interface {
		ApplyQueryTxSettingsOption(txSettings *Ydb_Query.TransactionSettings)
		ApplyTableTxSettingsOption(txSettings *Ydb_Table.TransactionSettings)
	}
	Settings []SettingsOption
)

func (opts Settings) applyTableTxSelector(txControl *Ydb_Table.TransactionControl) {
	beginTx := &Ydb_Table.TransactionSettings{}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyTableTxSettingsOption(beginTx)
		}
	}
	txControl.SetBeginTx(beginTx)
}

func (opts Settings) applyQueryTxSelector(txControl *Ydb_Query.TransactionControl) {
	beginTx := &Ydb_Query.TransactionSettings{}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyQueryTxSettingsOption(beginTx)
		}
	}
	txControl.SetBeginTx(beginTx)
}

func (opts Settings) ToYdbQuerySettings() *Ydb_Query.TransactionSettings {
	txSettings := &Ydb_Query.TransactionSettings{}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyQueryTxSettingsOption(txSettings)
		}
	}

	return txSettings
}

func (opts Settings) ToYdbTableSettings() *Ydb_Table.TransactionSettings {
	txSettings := &Ydb_Table.TransactionSettings{}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyTableTxSettingsOption(txSettings)
		}
	}

	return txSettings
}

// NewSettings returns transaction settings
func NewSettings(opts ...SettingsOption) Settings {
	return opts
}

func WithDefaultTxMode() SettingsOption {
	return WithSerializableReadWrite()
}

var _ SettingsOption = serializableReadWriteTxSettingsOption{}

type serializableReadWriteTxSettingsOption struct{}

func (serializableReadWriteTxSettingsOption) ApplyTableTxSettingsOption(settings *Ydb_Table.TransactionSettings) {
	settings.SetSerializableReadWrite(&Ydb_Table.SerializableModeSettings{})
}

func (serializableReadWriteTxSettingsOption) ApplyQueryTxSettingsOption(settings *Ydb_Query.TransactionSettings) {
	settings.SetSerializableReadWrite(&Ydb_Query.SerializableModeSettings{})
}

func WithSerializableReadWrite() SettingsOption {
	return serializableReadWriteTxSettingsOption{}
}

var _ SettingsOption = snapshotReadOnlyTxSettingsOption{}

type snapshotReadOnlyTxSettingsOption struct{}

func (snapshotReadOnlyTxSettingsOption) ApplyTableTxSettingsOption(settings *Ydb_Table.TransactionSettings) {
	settings.SetSnapshotReadOnly(&Ydb_Table.SnapshotModeSettings{})
}

func (snapshotReadOnlyTxSettingsOption) ApplyQueryTxSettingsOption(settings *Ydb_Query.TransactionSettings) {
	settings.SetSnapshotReadOnly(&Ydb_Query.SnapshotModeSettings{})
}

func WithSnapshotReadOnly() SettingsOption {
	return snapshotReadOnlyTxSettingsOption{}
}

var _ SettingsOption = snapshotReadWriteTxSettingsOption{}

type snapshotReadWriteTxSettingsOption struct{}

func (snapshotReadWriteTxSettingsOption) ApplyTableTxSettingsOption(settings *Ydb_Table.TransactionSettings) {
	settings.SetSnapshotReadWrite(&Ydb_Table.SnapshotRWModeSettings{})
}

func (snapshotReadWriteTxSettingsOption) ApplyQueryTxSettingsOption(settings *Ydb_Query.TransactionSettings) {
	settings.SetSnapshotReadWrite(&Ydb_Query.SnapshotRWModeSettings{})
}

func WithSnapshotReadWrite() SettingsOption {
	return snapshotReadWriteTxSettingsOption{}
}

var _ SettingsOption = staleReadOnlySettingsOption{}

type staleReadOnlySettingsOption struct{}

func (staleReadOnlySettingsOption) ApplyTableTxSettingsOption(settings *Ydb_Table.TransactionSettings) {
	settings.SetStaleReadOnly(&Ydb_Table.StaleModeSettings{})
}

func (staleReadOnlySettingsOption) ApplyQueryTxSettingsOption(settings *Ydb_Query.TransactionSettings) {
	settings.SetStaleReadOnly(&Ydb_Query.StaleModeSettings{})
}

func WithStaleReadOnly() SettingsOption {
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

var _ SettingsOption = onlineReadOnlySettingsOption{}

type onlineReadOnlySettingsOption []OnlineReadOnlyOption

func (opts onlineReadOnlySettingsOption) ApplyQueryTxSettingsOption(settings *Ydb_Query.TransactionSettings) {
	var ro onlineReadOnly
	for _, opt := range opts {
		if opt != nil {
			opt.applyTxOnlineReadOnlyOption(&ro)
		}
	}
	if ro {
		settings.SetOnlineReadOnly(Ydb_Query.OnlineModeSettings_builder{AllowInconsistentReads: true}.Build())
	} else {
		settings.SetOnlineReadOnly(Ydb_Query.OnlineModeSettings_builder{AllowInconsistentReads: false}.Build())
	}
}

func (opts onlineReadOnlySettingsOption) ApplyTableTxSettingsOption(settings *Ydb_Table.TransactionSettings) {
	var ro onlineReadOnly
	for _, opt := range opts {
		if opt != nil {
			opt.applyTxOnlineReadOnlyOption(&ro)
		}
	}
	if ro {
		settings.SetOnlineReadOnly(Ydb_Table.OnlineModeSettings_builder{AllowInconsistentReads: true}.Build())
	} else {
		settings.SetOnlineReadOnly(Ydb_Table.OnlineModeSettings_builder{AllowInconsistentReads: false}.Build())
	}
}

func WithOnlineReadOnly(opts ...OnlineReadOnlyOption) onlineReadOnlySettingsOption {
	return opts
}