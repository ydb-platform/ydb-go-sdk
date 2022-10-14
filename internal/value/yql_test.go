//go:build !fast
// +build !fast

package value

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestYQLTypeToString(t *testing.T) {
	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []Type{
		Void(),
		Null(),
		TypeBool,
		TypeInt8,
		TypeUint8,
		TypeInt16,
		TypeUint16,
		TypeInt32,
		TypeUint32,
		TypeInt64,
		TypeUint64,
		TypeFloat,
		TypeDouble,
		TypeDate,
		TypeDatetime,
		TypeTimestamp,
		TypeInterval,
		TypeTzDate,
		TypeTzDatetime,
		TypeTzTimestamp,
		TypeBytes,
		TypeText,
		TypeYSON,
		TypeJSON,
		TypeUUID,
		TypeJSONDocument,
		TypeDyNumber,
		Optional(TypeBool),
		Optional(TypeInt8),
		Optional(TypeUint8),
		Optional(TypeInt16),
		Optional(TypeUint16),
		Optional(TypeInt32),
		Optional(TypeUint32),
		Optional(TypeInt64),
		Optional(TypeUint64),
		Optional(TypeFloat),
		Optional(TypeDouble),
		Optional(TypeDate),
		Optional(TypeDatetime),
		Optional(TypeTimestamp),
		Optional(TypeInterval),
		Optional(TypeTzDate),
		Optional(TypeTzDatetime),
		Optional(TypeTzTimestamp),
		Optional(TypeBytes),
		Optional(TypeText),
		Optional(TypeYSON),
		Optional(TypeJSON),
		Optional(TypeUUID),
		Optional(TypeJSONDocument),
		Optional(TypeDyNumber),
		Decimal(22, 9),
		Dict(TypeText, TypeTimestamp),
		EmptyList(),
		List(TypeUint32),
		Variant(Struct(
			StructField{
				Name: "a",
				T:    TypeBool,
			},
			StructField{
				Name: "b",
				T:    TypeFloat,
			},
		)),
		Variant(Tuple(
			TypeBool,
			TypeFloat,
		)),
	} {
		t.Run(tt.String(), func(t *testing.T) {
			var got string
			err := retry.Do(context.Background(), db, func(ctx context.Context, cc *sql.Conn) error {
				row := cc.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ScriptingQueryMode),
					fmt.Sprintf("SELECT FormatType(ParseType(\"%s\"))", tt.String()),
				)
				if err := row.Scan(&got); err != nil {
					return err
				}
				return row.Err()
			})
			require.NoError(t, err)
			if got != tt.String() {
				t.Errorf("s representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.String())
			}
		})
	}
}

func TestYQLValueToString(t *testing.T) {
	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		value  Value
		string string
	}{
		{
			value:  VoidValue(),
			string: "FIXME",
		},
		{
			value:  TextValue("some\"text\"with brackets"),
			string: "CAST(\"some\\\"text\\\"with brackets\" AS Text)",
		},
		{
			value:  BytesValue([]byte("foo")),
			string: "CAST(\"foo\" AS Bytes)",
		},
		{
			value:  OptionalValue(BytesValue([]byte("foo"))),
			string: "CAST(\"foo\" AS Optional<Bytes>)",
		},
		{
			value:  BoolValue(true),
			string: "CAST(true AS Bool)",
		},
		{
			value:  Int8Value(42),
			string: "CAST(42 AS Int8)",
		},
		{
			value:  Uint8Value(42),
			string: "CAST(42 AS Uint8)",
		},
		{
			value:  Int16Value(42),
			string: "CAST(42 AS Int16)",
		},
		{
			value:  Uint16Value(42),
			string: "CAST(42 AS Uint16)",
		},
		{
			value:  Int32Value(42),
			string: "CAST(42 AS Int32)",
		},
		{
			value:  Uint32Value(42),
			string: "CAST(42 AS Uint32)",
		},
		{
			value:  Int64Value(42),
			string: "CAST(42 AS Int64)",
		},
		{
			value:  Uint64Value(42),
			string: "CAST(42 AS Uint64)",
		},
		{
			value:  FloatValue(42.2121236),
			string: "CAST(42.2121236 AS Float)",
		},
		{
			value:  DoubleValue(42.2121236192),
			string: "CAST(42.2121236192 AS Double)",
		},
		{
			value: DateValue(func() uint32 {
				v, _ := time.Parse("2006-01-02", "2022-06-17")
				return uint32(v.Sub(time.Unix(0, 0)) / time.Hour / 24)
			}()),
			string: "CAST(\"2022-06-17\" AS Date)",
		},
		{
			value: DatetimeValue(func() uint32 {
				v, _ := time.ParseInLocation("2006-01-02 15:04:05", "2022-06-17 05:19:20", time.Local)
				return uint32(v.Sub(time.Unix(0, 0)).Seconds())
			}()),
			string: "2022-06-17T05:19:20",
		},
		{
			value:  TzDateValue("2022-06-17,Europe/Berlin"),
			string: "2022-06-17,Europe/Berlin",
		},
		{
			value:  TzDatetimeValue("2022-06-17T05:19:20,Europe/Berlin"),
			string: "2022-06-17T05:19:20,Europe/Berlin",
		},
		{
			value:  IntervalValueFromDuration(time.Duration(42) * time.Millisecond),
			string: "42ms",
		},
		{
			value: TimestampValueFromTime(func() time.Time {
				tt, err := time.ParseInLocation(LayoutTimestamp, "1997-12-14T03:09:42.123456", time.Local)
				require.NoError(t, err)
				return tt.Local()
			}()),
			string: "1997-12-14T03:09:42.123456",
		},
		{
			value:  TzTimestampValue("1997-12-14T03:09:42.123456,Europe/Berlin"),
			string: "1997-12-14T03:09:42.123456,Europe/Berlin",
		},
		{
			value:  NullValue(TypeInt32),
			string: "NULL",
		},
		{
			value:  NullValue(Optional(TypeBool)),
			string: "NULL",
		},
		{
			value:  OptionalValue(OptionalValue(Int32Value(42))),
			string: "42",
		},
		{
			value:  OptionalValue(OptionalValue(OptionalValue(Int32Value(42)))),
			string: "42",
		},
		{
			value: ListValue(
				Int32Value(0),
				Int32Value(1),
				Int32Value(2),
				Int32Value(3),
			),
			string: "[0,1,2,3]",
		},
		{
			value: TupleValue(
				Int32Value(0),
				Int64Value(1),
				FloatValue(2),
				TextValue("3"),
			),
			string: "(0,1,2,\"3\")",
		},
		{
			value: VariantValue(Int32Value(42), 1, Variant(Tuple(
				TypeBytes,
				TypeInt32,
			))),
			string: "{1:42}",
		},
		{
			value: VariantValue(TextValue("foo"), 1, Variant(Tuple(
				TypeBytes,
				TypeText,
			))),
			string: "{1:\"foo\"}",
		},
		{
			value: VariantValue(BoolValue(true), 0, Variant(Tuple(
				TypeBytes,
				TypeInt32,
			))),
			string: "{0:true}",
		},
		{
			value: VariantValue(Int32Value(42), 1, Variant(Struct(
				StructField{"foo", TypeBytes},
				StructField{"bar", TypeInt32},
			))),
			string: "{1:42}",
		},
		{
			value: StructValue(
				StructValueField{"series_id", Uint64Value(1)},
				StructValueField{"title", TextValue("test")},
				StructValueField{"air_date", DateValue(1)},
			),
			string: "{\"series_id\":1,\"title\":\"test\",\"air_date\":\"1970-01-02\"}",
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), Int32Value(42)},
				DictValueField{TextValue("bar"), Int32Value(43)},
			),
			string: "{\"foo\":42,\"bar\":43}",
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), VoidValue()},
				DictValueField{TextValue("bar"), VoidValue()},
			),
			string: "{\"foo\":VOID,\"bar\":VOID}",
		},
		{
			value:  ZeroValue(TypeBool),
			string: "false",
		},
		{
			value:  ZeroValue(Optional(TypeBool)),
			string: "NULL",
		},
		{
			value:  ZeroValue(List(TypeBool)),
			string: "[]",
		},
		{
			value:  ZeroValue(Tuple(TypeBool, TypeDouble)),
			string: "(false,0)",
		},
		{
			value: ZeroValue(Struct(
				StructField{"foo", TypeBool},
				StructField{"bar", TypeText},
			)),
			string: "{\"foo\":false,\"bar\":\"\"}",
		},
		{
			value:  ZeroValue(Dict(TypeText, TypeTimestamp)),
			string: "{}",
		},
		{
			value:  ZeroValue(TypeUUID),
			string: "\"00000000000000000000000000000000\"",
		},
		{
			value:  DecimalValueFromBigInt(big.NewInt(-1234567890123456), 22, 9),
			string: "-1234567890123456",
		},
		{
			value:  DyNumberValue("-1234567890123456"),
			string: "-1234567890123456",
		},
		{
			value:  JSONValue("{\"a\":-1234567890123456}"),
			string: "{\"a\":-1234567890123456}",
		},
		{
			value:  JSONDocumentValue("{\"a\":-1234567890123456}"),
			string: "{\"a\":-1234567890123456}",
		},
		{
			value:  YSONValue([]byte("{\"a\":-1234567890123456}")),
			string: "{\"a\":-1234567890123456}",
		},
	} {
		t.Run(fmt.Sprintf("%+v", tt.value), func(t *testing.T) {
			var got string
			err := retry.Do(context.Background(), db, func(ctx context.Context, cc *sql.Conn) error {
				row := cc.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ScriptingQueryMode),
					fmt.Sprintf("SELECT %s)", tt.value.String()),
				)
				if err := row.Scan(&got); err != nil {
					return err
				}
				return row.Err()
			})
			require.NoError(t, err)
			if got != tt.String() {
				t.Errorf("s representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.String())
			}
		})
	}
}
