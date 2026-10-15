package arrow

import (
	"encoding/binary"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func computedColumn(a Array, inner types.Type, optional bool, read func(int) value.Value) decodedColumn {
	if optional {
		null := value.NullValue(inner)
		nonNull := read
		read = func(row int) value.Value {
			if a.IsNull(row) {
				return null
			}

			return value.OptionalValue(nonNull(row))
		}
	}

	return decodedColumn{value: read, scan: func(row int, dst any) error { return value.CastTo(read(row), dst) }}
}

//nolint:funlen,gocyclo // Logical types are mapped to their physical Arrow scalars in one place.
func logicalScalar(a Array, t types.Type) (func(int) value.Value, bool) {
	switch t {
	case types.Date:
		if data, ok := a.(scalar[uint16]); ok {
			return func(row int) value.Value { return value.DateValue(uint32(data.Value(row))) }, true
		}
	case types.Datetime:
		if data, ok := a.(scalar[uint32]); ok {
			return func(row int) value.Value { return value.DatetimeValue(data.Value(row)) }, true
		}
	case types.Timestamp:
		if data, ok := a.(scalar[uint64]); ok {
			return func(row int) value.Value { return value.TimestampValue(data.Value(row)) }, true
		}
	case types.Interval:
		if data, ok := a.(scalar[int64]); ok {
			return func(row int) value.Value { return value.IntervalValue(data.Value(row)) }, true
		}
	case types.Date32:
		if data, ok := a.(scalar[int32]); ok {
			return func(row int) value.Value { return value.Date32Value(data.Value(row)) }, true
		}
	case types.Datetime64:
		if data, ok := a.(scalar[int64]); ok {
			return func(row int) value.Value { return value.Datetime64Value(data.Value(row)) }, true
		}
	case types.Timestamp64:
		if data, ok := a.(scalar[int64]); ok {
			return func(row int) value.Value { return value.Timestamp64Value(data.Value(row)) }, true
		}
	case types.Interval64:
		if data, ok := a.(scalar[int64]); ok {
			return func(row int) value.Value { return value.Interval64Value(data.Value(row)) }, true
		}
	case types.JSON:
		if data, ok := a.(scalar[string]); ok {
			return func(row int) value.Value { return value.JSONValue(strings.Clone(data.Value(row))) }, true
		}
	case types.JSONDocument:
		if data, ok := a.(scalar[string]); ok {
			return func(row int) value.Value { return value.JSONDocumentValue(strings.Clone(data.Value(row))) }, true
		}
	case types.DyNumber:
		if data, ok := a.(scalar[string]); ok {
			return func(row int) value.Value { return value.DyNumberValue(strings.Clone(data.Value(row))) }, true
		}
	case types.YSON:
		if data, ok := a.(scalar[[]byte]); ok {
			return func(row int) value.Value { return value.YSONValue(append([]byte(nil), data.Value(row)...)) }, true
		}
	case types.UUID:
		if data, ok := a.(scalar[[]byte]); ok {
			return func(row int) value.Value {
				bytes := data.Value(row)

				return value.UUIDFromYDBPair(binary.LittleEndian.Uint64(bytes[8:]), binary.LittleEndian.Uint64(bytes[:8]))
			}, true
		}
	}
	switch t := t.(type) {
	case *types.Decimal:
		if data, ok := a.(scalar[[]byte]); ok {
			return func(row int) value.Value {
				bytes := data.Value(row)

				return value.DecimalValue(value.BigEndianUint128(
					binary.LittleEndian.Uint64(bytes[8:]), binary.LittleEndian.Uint64(bytes[:8]),
				), t.Precision(), t.Scale())
			}, true
		}
	case *types.PgType:
		if data, ok := a.(scalar[string]); ok {
			return func(row int) value.Value {
				if a.IsNull(row) {
					return value.PgNullValue(t.OID)
				}

				return value.PgValue(t.OID, strings.Clone(data.Value(row)))
			}, true
		}
	}

	return nil, false
}
