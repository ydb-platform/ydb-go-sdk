package arrow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestLogicalColumns(t *testing.T) {
	for _, test := range []struct {
		array Array
		want  value.Value
	}{
		{&scalarTestArray[uint16]{values: []uint16{42}}, value.DateValue(42)},
		{&scalarTestArray[uint32]{values: []uint32{42}}, value.DatetimeValue(42)},
		{&scalarTestArray[uint64]{values: []uint64{42}}, value.TimestampValue(42)},
		{&scalarTestArray[int64]{values: []int64{-42}}, value.IntervalValue(-42)},
		{&scalarTestArray[int32]{values: []int32{-42}}, value.Date32Value(-42)},
		{&scalarTestArray[int64]{values: []int64{-42}}, value.Datetime64Value(-42)},
		{&scalarTestArray[int64]{values: []int64{-42}}, value.Timestamp64Value(-42)},
		{&scalarTestArray[int64]{values: []int64{-42}}, value.Interval64Value(-42)},
		{&scalarTestArray[string]{values: []string{`{"key":42}`}}, value.JSONValue(`{"key":42}`)},
		{&scalarTestArray[string]{values: []string{`{"key":42}`}}, value.JSONDocumentValue(`{"key":42}`)},
		{&scalarTestArray[string]{values: []string{"1.42e2"}}, value.DyNumberValue("1.42e2")},
		{&scalarTestArray[[]byte]{values: [][]byte{[]byte("[42]")}}, value.YSONValue([]byte("[42]"))},
		{
			&scalarTestArray[[]byte]{values: [][]byte{{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}}},
			value.UUIDFromYDBPair(2, 1),
		},
		{
			&scalarTestArray[[]byte]{values: [][]byte{{42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}},
			value.DecimalValue(value.BigEndianUint128(0, 42), 22, 9),
		},
		{&scalarTestArray[string]{values: []string{"text"}}, value.PgValue(25, "text")},
		{&scalarTestArray[string]{values: []string{""}, nulls: []bool{true}}, value.PgNullValue(25)},
	} {
		t.Run(test.want.Type().Yql()+"/"+test.want.Yql(), func(t *testing.T) {
			column, err := newColumn(test.array, types.TypeFromYDB(test.want.Type().ToYDB()))
			require.NoError(t, err)
			require.Equal(t, test.want.Yql(), column.value(0).Yql())
			var scanned value.Value
			require.NoError(t, column.scan(0, &scanned))
			require.True(t, proto.Equal(value.ToYDB(test.want), value.ToYDB(scanned)))
		})
	}
}
