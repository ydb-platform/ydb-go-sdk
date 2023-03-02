//go:build !fast
// +build !fast

package integration

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/decimal"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

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
				if err = res.NextResultSetErr(ctx); err != nil {
					return err
				}
				if !res.NextRow() {
					return fmt.Errorf("unexpected no rows in result set (err = %w)", res.Err())
				}
				var v types.Value
				if err = res.Scan(&v); err != nil {
					return err
				}
				if tt.Yql() != v.Yql() {
					return fmt.Errorf("unexpected YQL: %T (%s) vs %T (%s) (err = %w)", tt, tt.Yql(), v, v.Yql(), res.Err())
				}
				return res.Err()
			}, table.WithIdempotent())
			require.NoError(t, err)
		})
	}
}
