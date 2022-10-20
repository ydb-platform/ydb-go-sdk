//go:build !fast
// +build !fast

package types_test

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/decimal"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestTypeToString(t *testing.T) {
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
			var got string
			err := retry.Do(context.Background(), db, func(ctx context.Context, cc *sql.Conn) error {
				row := cc.QueryRowContext(ctx,
					fmt.Sprintf("SELECT FormatType(ParseType(\"%s\"))", tt.Yql()),
				)
				if err := row.Scan(&got); err != nil {
					return err
				}
				return row.Err()
			})
			require.NoError(t, err)
			if got != tt.Yql() {
				t.Errorf("s representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.Yql())
			}
		})
	}
}

func TestValueToYqlLiteral(t *testing.T) {
	ctx := context.Background()
	db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()
	for i, tt := range []types.Value{
		types.VoidValue(),
		types.TextValue("some\"text\"with brackets"),
		types.TextValue(`some text with slashes \ \\ \\\`),
		types.BytesValue([]byte("foo")),
		types.OptionalValue(types.BytesValue([]byte("foo"))),
		types.BoolValue(true),
		types.Int8Value(42),
		types.Uint8Value(42),
		types.Int16Value(42),
		types.Uint16Value(42),
		types.Int32Value(42),
		types.Uint32Value(42),
		types.Int64Value(42),
		types.Uint64Value(42),
		types.Uint64Value(200000000000),
		types.FloatValue(42.2121236),
		types.FloatValue(float32(math.Inf(+1))),
		types.FloatValue(float32(math.Inf(-1))),
		types.FloatValue(float32(math.NaN())),
		types.DoubleValue(42.2121236192),
		types.DoubleValue(math.Inf(+1)),
		types.DoubleValue(math.Inf(-1)),
		types.DoubleValue(math.NaN()),
		types.DateValue(func() uint32 {
			v, _ := time.Parse("2006-01-02", "2022-06-17")
			return uint32(v.Sub(time.Unix(0, 0)) / time.Hour / 24)
		}()),
		types.DatetimeValue(func() uint32 {
			v, _ := time.ParseInLocation("2006-01-02 15:04:05", "2022-06-17 05:19:20", time.Local)
			return uint32(v.Sub(time.Unix(0, 0)).Seconds())
		}()),
		types.TzDateValue("2022-06-17,Europe/Berlin"),
		types.TzDatetimeValue("2022-06-17T05:19:20,Europe/Berlin"),
		types.IntervalValueFromDuration(
			-(123329*time.Hour + 893745*time.Second + 42*time.Millisecond + time.Microsecond*666),
		),
		types.TimestampValueFromTime(func() time.Time {
			tt, err := time.ParseInLocation(
				"2006-01-02T15:04:05.000000",
				"1997-12-14T03:09:42.123456",
				time.Local,
			)
			require.NoError(t, err)
			return tt.Local()
		}()),
		types.TzTimestampValue("1997-12-14T03:09:42.123456,Europe/Berlin"),
		types.NullValue(types.TypeInt32),
		types.NullValue(types.Optional(types.TypeBool)),
		types.OptionalValue(types.OptionalValue(types.Int32Value(42))),
		types.OptionalValue(types.OptionalValue(types.OptionalValue(types.Int32Value(42)))),
		types.ListValue(
			types.Int32Value(0),
			types.Int32Value(1),
			types.Int32Value(2),
			types.Int32Value(3),
		),
		types.SetValue(
			types.Int32Value(0),
			types.Int32Value(1),
			types.Int32Value(2),
			types.Int32Value(3),
		),
		types.TupleValue(
			types.Int32Value(0),
			types.Int64Value(1),
			types.FloatValue(2),
			types.TextValue("3"),
		),
		types.VariantValueTuple(types.Int32Value(42), 1, types.VariantTuple(
			types.TypeBytes,
			types.TypeInt32,
		)),
		types.VariantValueTuple(types.TextValue("foo"), 1, types.VariantTuple(
			types.TypeBytes,
			types.TypeText,
		)),
		types.VariantValueTuple(types.BoolValue(true), 0, types.VariantTuple(
			types.TypeBool,
			types.TypeInt32,
		)),
		types.VariantValueStruct(types.Int32Value(42), "bar", types.VariantStruct(
			types.StructField("foo", types.TypeBytes),
			types.StructField("bar", types.TypeInt32),
		)),
		types.VariantValueStruct(types.Int32Value(6), "foo", types.VariantStruct(
			types.StructField("foo", types.TypeInt32),
			types.StructField("bar", types.TypeBool),
		)),
		types.StructValue(
			types.StructFieldValue("series_id", types.Uint64Value(1)),
			types.StructFieldValue("title", types.TextValue("test")),
			types.StructFieldValue("air_date", types.DateValue(1)),
		),
		types.DictValue(
			types.DictFieldValue(types.TextValue("foo"), types.Int32Value(42)),
			types.DictFieldValue(types.TextValue("bar"), types.Int32Value(43)),
		),
		types.DictValue(
			types.DictFieldValue(types.TextValue("foo"), types.FloatValue(123)),
			types.DictFieldValue(types.TextValue("bar"), types.FloatValue(456)),
		),
		types.ZeroValue(types.TypeBool),
		types.ZeroValue(types.Optional(types.TypeBool)),
		types.ZeroValue(types.Tuple(types.TypeBool, types.TypeDouble)),
		types.ZeroValue(types.Struct(
			types.StructField("foo", types.TypeBool),
			types.StructField("bar", types.TypeText),
		)),
		types.ZeroValue(types.TypeUUID),
		func() types.Value {
			v, err := decimal.Parse("-237893478741.23893477", 22, 8)
			if err != nil {
				panic(err)
			}
			return types.DecimalValueFromBigInt(v, 22, 8)
		}(),
		types.DyNumberValue("-.1234567890123456e16"),
		types.JSONValue("{\"a\":1,\"b\":null}"),
		types.JSONDocumentValue("{\"a\":1,\"b\":null}"),
		types.YSONValue("<a=1>[3;%false]"),
	} {
		t.Run(strconv.Itoa(i)+"."+tt.Yql(), func(t *testing.T) {
			err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
				if i == 28 {
					i = 28
				}
				res, err := tx.Execute(ctx, fmt.Sprintf("SELECT %s;", tt.Yql()), nil)
				if err != nil {
					return err
				}
				require.NoError(t, res.NextResultSetErr(ctx))
				require.True(t, res.NextRow())
				values, err := res.(interface {
					RowValues() ([]types.Value, error)
				}).RowValues()
				require.NoError(t, err)
				require.Equal(t, 1, len(values))
				require.Equal(t, tt.Yql(), values[0].Yql(), fmt.Sprintf("%T vs %T", tt, values[0]))
				return nil
			})
			require.NoError(t, err)
		})
	}
}
