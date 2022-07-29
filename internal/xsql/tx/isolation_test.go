package tx

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func TestIsolationMapping(t *testing.T) {
	for _, test := range []struct {
		name   string
		opts   driver.TxOptions
		txExp  table.TxOption
		txcExp []table.TxControlOption
		err    bool
	}{
		{
			name: "default",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  false,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "serializable",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  false,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "linearizable",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  false,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "default ro",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  true,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "serializable ro",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  true,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "linearizable ro",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  true,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "read uncommitted",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadUncommitted),
				ReadOnly:  true,
			},
			txcExp: []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(
						table.WithInconsistentReads(),
					),
				),
				table.CommitTx(),
			},
		},
		{
			name: "read committed",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadCommitted),
				ReadOnly:  true,
			},
			txcExp: []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(),
				),
				table.CommitTx(),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			txAct, txcAct, err := isolationOrControl(test.opts)
			if !test.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if test.err && err == nil {
				t.Fatalf("expected error; got nil")
			}

			var sAct, sExp *table.TransactionSettings
			if txAct != nil {
				sAct = table.TxSettings(txAct)
			}
			if test.txExp != nil {
				sExp = table.TxSettings(test.txExp)
			}
			if !proto.Equal(sAct.Settings(), sExp.Settings()) {
				t.Fatalf("unexpected tx settings: %+v; want %+v", sAct, sExp)
			}

			var cAct, cExp *table.TransactionControl
			if txcAct != nil {
				cAct = table.TxControl(txcAct...)
			}
			if test.txcExp != nil {
				cExp = table.TxControl(test.txcExp...)
			}
			if !proto.Equal(cAct.Desc(), cExp.Desc()) {
				t.Fatalf("unexpected settings: %+v; want %+v", cAct, cExp)
			}
		})
	}
}
