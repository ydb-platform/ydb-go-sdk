package isolation

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// ToYDB maps driver transaction options to ydb transaction Option or query transaction control.
// This caused by ydb logic that prevents start actual transaction with OnlineReadOnly mode and ReadCommitted
// and ReadUncommitted isolation levels should use tx_control in every query request.
// It returns error on unsupported options.
func ToYDB(opts driver.TxOptions) (txControl table.TxOption, err error) {
	level := sql.IsolationLevel(opts.Isolation)
	if !opts.ReadOnly && level == sql.LevelDefault {
		return table.WithSerializableReadWrite(), nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf(
		"ydb: unsupported transaction options: isolation='%s' read_only='%t'",
		level.String(), opts.ReadOnly,
	))
}

// FromYDB maps table transaction settings to driver transaction options
func FromYDB(txSettings *table.TransactionSettings) (txOptions *sql.TxOptions, err error) {
	switch txSettings.Settings().TxMode.(type) {
	case *Ydb_Table.TransactionSettings_SerializableReadWrite:
		return &sql.TxOptions{
			Isolation: sql.LevelDefault,
			ReadOnly:  false,
		}, nil
	default:
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("ydb: unsupported transaction settings: %+v", txSettings),
		)
	}
}
