//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestTypeToString(t *testing.T) {
	ctx := xtest.Context(t)

	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()
	for _, tt := range []types.Type{
		types.Void(),
		types.TypeBool,
		types.TypeInt8,
		types.TypeUint8,
		types.TypeInt16,
		types.TypeUint16,
		types.TypeInt32,
		types.TypeUint32,
		types.TypeInt64,
		types.TypeUint64,
		types.TypeFloat,
		types.TypeDouble,
		types.TypeDate,
		types.TypeDatetime,
		types.TypeTimestamp,
		types.TypeInterval,
		types.TypeTzDate,
		types.TypeTzDatetime,
		types.TypeTzTimestamp,
		types.TypeBytes,
		types.TypeText,
		types.TypeYSON,
		types.TypeJSON,
		types.TypeUUID,
		types.TypeJSONDocument,
		types.TypeDyNumber,
		types.Optional(types.TypeBool),
		types.Optional(types.TypeInt8),
		types.Optional(types.TypeUint8),
		types.Optional(types.TypeInt16),
		types.Optional(types.TypeUint16),
		types.Optional(types.TypeInt32),
		types.Optional(types.TypeUint32),
		types.Optional(types.TypeInt64),
		types.Optional(types.TypeUint64),
		types.Optional(types.TypeFloat),
		types.Optional(types.TypeDouble),
		types.Optional(types.TypeDate),
		types.Optional(types.TypeDatetime),
		types.Optional(types.TypeTimestamp),
		types.Optional(types.TypeInterval),
		types.Optional(types.TypeTzDate),
		types.Optional(types.TypeTzDatetime),
		types.Optional(types.TypeTzTimestamp),
		types.Optional(types.TypeBytes),
		types.Optional(types.TypeText),
		types.Optional(types.TypeYSON),
		types.Optional(types.TypeJSON),
		types.Optional(types.TypeUUID),
		types.Optional(types.TypeJSONDocument),
		types.Optional(types.TypeDyNumber),
		types.Dict(types.TypeText, types.TypeTimestamp),
		types.List(types.TypeUint32),
		types.VariantStruct(
			types.StructField("a", types.TypeBool),
			types.StructField("b", types.TypeFloat),
		),
		types.VariantTuple(
			types.TypeBool,
			types.TypeFloat,
		),
	} {
		t.Run(tt.Yql(), func(t *testing.T) {
			var yql string
			err := retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
				row := cc.QueryRowContext(ctx,
					fmt.Sprintf("SELECT FormatType(ParseType(\"%s\"))", tt.Yql()),
				)
				if err := row.Scan(&yql); err != nil {
					return err
				}
				return row.Err()
			})
			require.NoError(t, err)
			require.Equal(t, tt.Yql(), yql)
		})
	}
}
