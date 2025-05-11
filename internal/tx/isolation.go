package tx

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

type Isolation int

func (iso Isolation) ToTableSettings() *Ydb_Table.TransactionSettings {
	return iso.tableTxSelector().BeginTx
}

func (iso Isolation) ToQuerySettings() *Ydb_Query.TransactionSettings {
	return iso.queryTxSelector().BeginTx
}

func (iso Isolation) applyQueryTxSelector(txControl *Ydb_Query.TransactionControl) {
	txControl.TxSelector = iso.queryTxSelector()
}

func (iso Isolation) applyTableTxSelector(txControl *Ydb_Table.TransactionControl) {
	txControl.TxSelector = iso.tableTxSelector()
}

func (iso Isolation) applyTxControlOption(txControl *Control) {
	txControl.selector = iso
}

func (iso Isolation) tableTxSelector() *Ydb_Table.TransactionControl_BeginTx {
	switch iso {
	case SerializableRW:
		return tableSerializableReadWriteTxSelector
	case SnapshotRO:
		return tableSnapshotReadOnlyTxSelector
	case OnlineRO:
		return tableOnlineReadOnlyForbidInconsistentReadsTxSelector
	case OnlineROWithInconsistentReads:
		return tableOnlineReadOnlyAllowInconsistentReadsTxSelector
	case StaleRO:
		return tableStaleReadOnlyTxSelector
	default:
		panic(fmt.Sprintf("unknown isolation: %d", iso))
	}
}

func (iso Isolation) queryTxSelector() *Ydb_Query.TransactionControl_BeginTx {
	switch iso {
	case SerializableRW:
		return querySerializableReadWriteTxSelector
	case SnapshotRO:
		return querySnapshotReadOnlyTxSelector
	case OnlineRO:
		return queryOnlineReadOnlyForbidInconsistentReadsTxSelector
	case OnlineROWithInconsistentReads:
		return queryOnlineReadOnlyAllowInconsistentReadsTxSelector
	case StaleRO:
		return queryStaleReadOnlyTxSelector
	default:
		panic(fmt.Sprintf("unknown isolation: %d", iso))
	}
}

const (
	SerializableRW = Isolation(iota)
	SnapshotRO
	OnlineRO
	OnlineROWithInconsistentReads
	StaleRO
)

var _ Settings = Isolation(0)
