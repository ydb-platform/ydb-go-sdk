//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestCreateTableWithSequence(sourceTest *testing.T) {
	scope := newScope(sourceTest)
	scope.TableName()
	db := scope.Driver()
	ctx := scope.Ctx
	startValue := int64(1000)

	tablePath := path.Join(scope.Folder(), "table")

	_ = db.Table().Do(scope.Ctx, func(ctx context.Context, session table.Session) error {
		return session.DropTable(ctx, tablePath)
	})

	// Create table with sequence
	err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, tablePath,
			options.WithColumnWithSequence("id", types.Optional(types.TypeInt32), options.Sequence{StartValue: startValue}),
			options.WithColumn("title", types.Optional(types.TypeText)),
			options.WithPrimaryKeyColumn("id"),
		)
	})
	scope.Require.NoError(err)

	writeTx := table.SerializableReadWriteTxControl(
		table.CommitTx(),
	)

	// Insert row without specifying id and use RETURNING to get the generated id
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		var res result.Result
		_, res, err = s.Execute(ctx, writeTx,
			fmt.Sprintf("INSERT INTO `%s` (title) VALUES (\"test value\") RETURNING id", tablePath), nil)
		if err != nil {
			return err
		}

		defer func() {
			_ = res.Close()
		}()

		scope.Require.True(res.NextResultSet(ctx))
		scope.Require.True(res.NextRow())

		var id int32
		err = res.ScanNamed(
			named.Required("id", &id),
		)
		scope.Require.NoError(err)
		scope.Require.NotNil(id)
		scope.Require.Equal(int32(startValue), id)

		return nil
	})
	scope.Require.NoError(err)
}
