package isolation

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// ToYDB maps driver transaction options to ydb transaction Option or query transaction control.
// This caused by ydb logic that prevents start actual transaction with OnlineReadOnly mode and ReadCommitted
// and ReadUncommitted isolation levels should use tx_control in every query request.
// It returns error on unsupported options.
func ToYDB(opts driver.TxOptions) (txcControl table.TxOption, err error) {
	level := sql.IsolationLevel(opts.Isolation)
	switch level {
	case sql.LevelDefault, sql.LevelSerializable:
		if !opts.ReadOnly {
			return table.WithSerializableReadWrite(), nil
		}
	case sql.LevelSnapshot:
		if opts.ReadOnly {
			return table.WithSnapshotReadOnly(), nil
		}
	}

	return nil, xerrors.WithStackTrace(fmt.Errorf(
		"unsupported transaction options: %+v", opts,
	))
}
