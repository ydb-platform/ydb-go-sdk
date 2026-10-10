package value

import (
	"bytes"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wirevalue"
)

// FromWire converts a protobuf-encoded YDB cell to an SDK value without
// constructing a protobuf Value tree. The column type can be reused for rows.
func FromWire(t types.Type, data []byte) (Value, error) {
	cell, err := wirevalue.Parse(data)
	if err != nil {
		return nil, err
	}

	return FromCell(t, cell)
}

// FromCell converts an already parsed cell using the column's SDK type.
func FromCell(t types.Type, cell wirevalue.Cell) (Value, error) {
	if cell.Kind() == wirevalue.ValueNullField {
		if v, ok := nullFromWire(t); ok {
			return v, nil
		}
	}
	switch t := t.(type) {
	case types.Primitive:
		return primitiveFromWire(t, cell)
	case types.Optional:
		inner := cell
		if cell.Kind() == wirevalue.ValueNestedField &&
			(wireTerminalNull(cell) || !variantPayload(t.InnerType())) {
			var err error
			inner, err = cell.Nested()
			if err != nil {
				return nil, err
			}
		}
		v, err := FromCell(t.InnerType(), inner)
		if err != nil {
			return nil, err
		}

		return OptionalValue(v), nil
	case *types.Tagged:
		v, err := FromCell(t.InnerType(), cell)
		if err != nil {
			return nil, err
		}

		return TaggedValue(t, v), nil
	case *types.Decimal:
		return DecimalValue(BigEndianUint128(cell.High128(), cell.Uint64()), t.Precision(), t.Scale()), nil
	case types.Void:
		return VoidValue(), nil
	case types.Null:
		return LiteralNullValue(), nil
	case types.EmptyList:
		return ListValueWithType(t, nil), nil
	case types.EmptyDict:
		return DictValueWithType(t, nil), nil
	case *types.PgType:
		if cell.Kind() == wirevalue.ValueNullField {
			return PgNullValue(t.OID), nil
		}
		v := PgValue(t.OID, string(cell.Bytes()))

		return &v, nil
	default:
		return compositeFromWire(t, cell)
	}
}

func nullFromWire(t types.Type) (Value, bool) {
	switch t := t.(type) {
	case types.Optional:
		return NullValue(t.InnerType()), true
	case types.Void:
		return VoidValue(), true
	default:
		return nil, false
	}
}

func compositeFromWire(t types.Type, cell wirevalue.Cell) (Value, error) {
	switch t := t.(type) {
	case *types.List:
		return listFromWire(t, cell)
	case *types.Tuple:
		return tupleFromWire(t, cell)
	case *types.Struct:
		return structFromWire(t, cell)
	case *types.Dict:
		return dictFromWire(t, cell)
	case *types.Set:
		return setFromWire(t, cell)
	case *types.VariantTuple:
		return variantTupleFromWire(t, cell)
	case *types.VariantStruct:
		return variantStructFromWire(t, cell)
	default:
		return nil, fmt.Errorf("unsupported YDB type %T", t)
	}
}

func variantTupleFromWire(t *types.VariantTuple, cell wirevalue.Cell) (Value, error) {
	index := int(cell.VariantIndex())
	if index >= len(t.Tuple.InnerTypes()) {
		return nil, fmt.Errorf("variant tuple index %d out of range", index)
	}
	inner, err := cell.Nested()
	if err != nil {
		return nil, err
	}
	v, err := FromCell(t.Tuple.ItemType(index), inner)
	if err != nil {
		return nil, err
	}

	return VariantValueTuple(v, cell.VariantIndex(), t.Tuple), nil
}

func variantStructFromWire(t *types.VariantStruct, cell wirevalue.Cell) (Value, error) {
	index := int(cell.VariantIndex())
	if index >= len(t.Struct.Fields()) {
		return nil, fmt.Errorf("variant struct index %d out of range", index)
	}
	field := t.Struct.Field(index)
	inner, err := cell.Nested()
	if err != nil {
		return nil, err
	}
	v, err := FromCell(field.T, inner)
	if err != nil {
		return nil, err
	}

	return VariantValueStruct(v, field.Name, t.Struct), nil
}

func wireTerminalNull(cell wirevalue.Cell) bool {
	for cell.Kind() == wirevalue.ValueNestedField {
		var err error
		cell, err = cell.Nested()
		if err != nil {
			return false
		}
	}

	return cell.Kind() == wirevalue.ValueNullField
}

func listFromWire(t *types.List, cell wirevalue.Cell) (Value, error) {
	values := make([]Value, 0)
	items := cell.Items()
	for items.Next() {
		v, err := FromCell(t.ItemType(), items.Cell())
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}

	return ListValueWithType(t, values), items.Err()
}

func tupleFromWire(t *types.Tuple, cell wirevalue.Cell) (Value, error) {
	values := make([]Value, 0, len(t.InnerTypes()))
	items := cell.Items()
	for items.Next() {
		if len(values) >= len(t.InnerTypes()) {
			return nil, fmt.Errorf("tuple has more than %d items", len(t.InnerTypes()))
		}
		v, err := FromCell(t.ItemType(len(values)), items.Cell())
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}

	return TupleValue(values...), items.Err()
}

func structFromWire(t *types.Struct, cell wirevalue.Cell) (Value, error) {
	fields := make([]StructValueField, 0, len(t.Fields()))
	items := cell.Items()
	for items.Next() {
		if len(fields) >= len(t.Fields()) {
			return nil, fmt.Errorf("struct has more than %d fields", len(t.Fields()))
		}
		field := t.Field(len(fields))
		v, err := FromCell(field.T, items.Cell())
		if err != nil {
			return nil, err
		}
		fields = append(fields, StructValueField{Name: field.Name, V: v})
	}

	return StructValue(fields...), items.Err()
}

func dictFromWire(t *types.Dict, cell wirevalue.Cell) (Value, error) {
	values := make([]DictValueField, 0)
	pairs := cell.Pairs()
	for pairs.Next() {
		key, err := FromCell(t.KeyType(), pairs.Key())
		if err != nil {
			return nil, err
		}
		payload, err := FromCell(t.ValueType(), pairs.Payload())
		if err != nil {
			return nil, err
		}
		values = append(values, DictValueField{K: key, V: payload})
	}

	return DictValueWithType(t, values), pairs.Err()
}

func setFromWire(t *types.Set, cell wirevalue.Cell) (Value, error) {
	values := make([]Value, 0)
	pairs := cell.Pairs()
	for pairs.Next() {
		v, err := FromCell(t.ItemType(), pairs.Key())
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}

	return SetValueWithType(t, values), pairs.Err()
}

//nolint:funlen,gocyclo // One schema type maps to one existing SDK constructor.
func primitiveFromWire(t types.Primitive, cell wirevalue.Cell) (Value, error) {
	switch t {
	case types.Bool:
		return BoolValue(cell.Uint64() != 0), nil
	case types.Int8:
		return Int8Value(int8(cell.Uint32())), nil
	case types.Int16:
		return Int16Value(int16(cell.Uint32())), nil
	case types.Int32:
		return Int32Value(int32(cell.Uint32())), nil
	case types.Int64:
		return Int64Value(int64(cell.Uint64())), nil
	case types.Uint8:
		return Uint8Value(uint8(cell.Uint32())), nil
	case types.Uint16:
		return Uint16Value(uint16(cell.Uint32())), nil
	case types.Uint32:
		return Uint32Value(cell.Uint32()), nil
	case types.Uint64:
		return Uint64Value(cell.Uint64()), nil
	case types.Date:
		return DateValue(cell.Uint32()), nil
	case types.Date32:
		return Date32Value(int32(cell.Uint32())), nil
	case types.Datetime:
		return DatetimeValue(cell.Uint32()), nil
	case types.Datetime64:
		return Datetime64Value(int64(cell.Uint64())), nil
	case types.Timestamp:
		return TimestampValue(cell.Uint64()), nil
	case types.Timestamp64:
		return Timestamp64Value(int64(cell.Uint64())), nil
	case types.Interval:
		return IntervalValue(int64(cell.Uint64())), nil
	case types.Interval64:
		return Interval64Value(int64(cell.Uint64())), nil
	case types.Float:
		return FloatValue(cell.Float32()), nil
	case types.Double:
		return DoubleValue(cell.Float64()), nil
	case types.Text:
		return TextValue(string(cell.Bytes())), nil
	case types.Bytes:
		return BytesValue(bytes.Clone(cell.Bytes())), nil
	case types.YSON:
		return YSONValue(bytes.Clone(cell.Bytes())), nil
	case types.JSON:
		return JSONValue(string(cell.Bytes())), nil
	case types.JSONDocument:
		return JSONDocumentValue(string(cell.Bytes())), nil
	case types.DyNumber:
		return DyNumberValue(string(cell.Bytes())), nil
	case types.TzDate:
		return TzDateValue(string(cell.Bytes())), nil
	case types.TzDatetime:
		return TzDatetimeValue(string(cell.Bytes())), nil
	case types.TzTimestamp:
		return TzTimestampValue(string(cell.Bytes())), nil
	case types.TzDate32:
		return TzDate32Value(string(cell.Bytes())), nil
	case types.TzDatetime64:
		return TzDatetime64Value(string(cell.Bytes())), nil
	case types.TzTimestamp64:
		return TzTimestamp64Value(string(cell.Bytes())), nil
	case types.UUID:
		return UUIDFromYDBPair(cell.High128(), cell.Uint64()), nil
	default:

		return nil, fmt.Errorf("unsupported YDB primitive %v", t)
	}
}
