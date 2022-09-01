package isolation

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func TestToYDB(t *testing.T) {
	for _, tt := range []struct {
		txOptions driver.TxOptions
		txControl table.TxOption
		err       bool
	}{
		// read-write
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  false,
			},
			txControl: table.WithSerializableReadWrite(),
			err:       false,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadUncommitted),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadCommitted),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelWriteCommitted),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelRepeatableRead),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSnapshot),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  false,
			},
			err: true,
		},

		// read-only
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadUncommitted),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadCommitted),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelWriteCommitted),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelRepeatableRead),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSnapshot),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  true,
			},
			err: true,
		},
	} {
		t.Run(fmt.Sprintf("%+v", tt.txOptions), func(t *testing.T) {
			toYDB, err := ToYDB(tt.txOptions)
			if !tt.err {
				require.NoError(t, err)
				if !proto.Equal(table.TxSettings(tt.txControl).Settings(), table.TxSettings(toYDB).Settings()) {
					t.Errorf("%+v != %+v", toYDB, tt.txControl)
				}
			} else {
				require.Error(t, err)
			}
		})
	}
}
