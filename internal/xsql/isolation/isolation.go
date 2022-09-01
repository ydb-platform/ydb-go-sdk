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
	if !opts.ReadOnly && level == sql.LevelDefault {
		return table.WithSerializableReadWrite(), nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf(
		"ydb: unsupported transaction options: %+v", opts,
	))
}
