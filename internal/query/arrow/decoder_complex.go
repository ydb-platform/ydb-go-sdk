package arrow

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type structArray[A Array] interface {
	NumField() int
	Field(index int) A
}

type listArray[A Array] interface {
	ListValues() A
}

type unionArray[A Array] interface {
	NumFields() int
	Field(index int) A
	ChildID(row int) int
}

func nullableColumn(t types.Type) bool {
	switch t := t.(type) {
	case types.Optional, types.Null, types.Void, *types.PgType:
		return true
	case *types.Tagged:
		return nullableColumn(t.InnerType())
	default:
		return false
	}
}

func optionalWrapper(t types.Type) bool {
	switch t := t.(type) {
	case types.Optional, types.Null, types.Void, types.EmptyList, types.EmptyDict,
		*types.PgType, *types.VariantTuple, *types.VariantStruct:
		return true
	case *types.Tagged:
		return optionalWrapper(t.InnerType())
	default:
		return false
	}
}

//nolint:funlen,gocyclo // Dispatch keeps the YDB container layout visible in one place.
func newComplexColumn[A Array](a A, t types.Type, optional bool, active func(int) bool) (decodedColumn, bool, error) {
	valid := func(row int) bool { return (active == nil || active(row)) && (!optional || !a.IsNull(row)) }
	if optional && optionalWrapper(t) {
		fields, ok := any(a).(structArray[A])
		if !ok || fields.NumField() != 1 {
			return decodedColumn{}, true, fmt.Errorf("expected Arrow optional wrapper for YDB %s", t)
		}
		inner, err := newColumnActive(fields.Field(0), t, valid)

		return computedColumn(a, t, true, inner.value), true, err
	}
	var read func(int) value.Value
	var err error
	switch t := t.(type) {
	case *types.Tagged:
		var inner decodedColumn
		inner, err = newColumnActive(a, t.InnerType(), valid)
		read = func(row int) value.Value { return value.TaggedValue(t, inner.value(row)) }
	case types.Null:
		if a.NullN() != a.Len() {
			err = fmt.Errorf("expected Arrow null array for YDB Null")
		}
		read = func(int) value.Value { return value.LiteralNullValue() }
	case types.Void, types.EmptyList, types.EmptyDict:
		fields, ok := any(a).(structArray[A])
		if !ok || fields.NumField() != 0 {
			err = fmt.Errorf("expected empty Arrow struct for YDB %s", t)
		}
		switch t.(type) {
		case types.Void:
			read = func(int) value.Value { return value.VoidValue() }
		case types.EmptyList:
			read = func(int) value.Value { return value.ListValue() }
		case types.EmptyDict:
			read = func(int) value.Value { return value.DictValue() }
		}
	case *types.Tuple:
		var columns []decodedColumn
		columns, err = structColumns(a, t.InnerTypes(), valid)
		read = func(row int) value.Value { return value.TupleValue(readFields(columns, row)...) }
	case *types.Struct:
		fields := t.Fields()
		typesInFields := make([]types.Type, len(fields))
		for i, field := range fields {
			typesInFields[i] = field.T
		}
		var columns []decodedColumn
		columns, err = structColumns(a, typesInFields, valid)
		read = func(row int) value.Value {
			values := make([]value.StructValueField, len(fields))
			for i, field := range fields {
				values[i] = value.StructValueField{Name: field.Name, V: columns[i].value(row)}
			}

			return value.StructValue(values...)
		}
	case *types.List:
		var child A
		var offsets func(int) (int64, int64)
		var childActive func(int) bool
		child, offsets, childActive, err = listData(a, valid)
		if err != nil {
			break
		}
		var column decodedColumn
		column, err = newColumnActive(child, t.ItemType(), childActive)
		read = func(row int) value.Value {
			start, end := offsets(row)
			items := make([]value.Value, int(end-start))
			for i := range items {
				items[i] = column.value(int(start) + i)
			}

			return value.ListValueWithType(t, items)
		}
	case *types.Dict:
		read, err = dictColumn(a, t, valid)
	case *types.Set:
		read, err = setColumn(a, t, valid)
	case *types.VariantTuple:
		read, err = variantColumn(a, t, t.InnerTypes(), valid)
	case *types.VariantStruct:
		fields := t.Fields()
		items := make([]types.Type, len(fields))
		for i, field := range fields {
			items[i] = field.T
		}
		read, err = variantColumn(a, t, items, valid)
	case types.Primitive:
		if !timezoneType(t) {
			return decodedColumn{}, false, nil
		}
		read, err = timezoneColumn(a, t, valid)
	default:
		return decodedColumn{}, false, nil
	}
	if err != nil {
		return decodedColumn{}, true, err
	}

	return computedColumn(a, t, optional, read), true, nil
}

func structColumns[A Array](a A, fields []types.Type, active func(int) bool) ([]decodedColumn, error) {
	data, ok := any(a).(structArray[A])
	if !ok || data.NumField() != len(fields) {
		return nil, fmt.Errorf("arrow struct has incompatible fields for YDB struct or tuple")
	}
	columns := make([]decodedColumn, len(fields))
	for i, t := range fields {
		var err error
		columns[i], err = newColumnActive(data.Field(i), t, active)
		if err != nil {
			return nil, fmt.Errorf("field %d: %w", i, err)
		}
	}

	return columns, nil
}

func readFields(columns []decodedColumn, row int) []value.Value {
	items := make([]value.Value, len(columns))
	for i, column := range columns {
		items[i] = column.value(row)
	}

	return items
}

func listData[A Array](a A, active func(int) bool) (
	child A, offsets func(int) (int64, int64), childActive func(int) bool, err error,
) {
	data, ok := any(a).(listArray[A])
	if !ok {
		return child, nil, nil, fmt.Errorf("expected Arrow list")
	}
	child = data.ListValues()
	switch data := any(a).(type) {
	case interface{ ValueOffsets(row int) (int64, int64) }:
		offsets = data.ValueOffsets
	case interface{ Offsets() []int32 }:
		// Legacy IPC readers return arrays with zero logical offset.
		values := data.Offsets()
		if len(values) < a.Len()+1 {
			return child, nil, nil, fmt.Errorf("invalid Arrow list offsets")
		}
		offsets = func(row int) (int64, int64) { return int64(values[row]), int64(values[row+1]) }
	default:
		return child, nil, nil, fmt.Errorf("arrow list does not expose offsets")
	}
	used := make([]bool, child.Len())
	for row := 0; row < a.Len(); row++ {
		start, end := offsets(row)
		if start < 0 || end < start || end > int64(child.Len()) {
			return child, nil, nil, fmt.Errorf("invalid Arrow list offsets at row %d", row)
		}
		if active(row) {
			for i := start; i < end; i++ {
				used[i] = true
			}
		}
	}

	return child, offsets, func(row int) bool { return used[row] }, nil
}

func dictColumn[A Array](a A, t *types.Dict, active func(int) bool) (func(int) value.Value, error) {
	child, offsets, used, err := listData(a, active)
	if err != nil {
		return nil, err
	}
	columns, err := structColumns(child, []types.Type{t.KeyType(), t.ValueType()}, used)
	if err != nil {
		return nil, err
	}

	return func(row int) value.Value {
		start, end := offsets(row)
		pairs := make([]value.DictValueField, int(end-start))
		for i := range pairs {
			pairs[i] = value.DictValueField{K: columns[0].value(int(start) + i), V: columns[1].value(int(start) + i)}
		}

		return value.DictValueWithType(t, pairs)
	}, nil
}

func setColumn[A Array](a A, t *types.Set, active func(int) bool) (func(int) value.Value, error) {
	child, offsets, used, err := listData(a, active)
	if err != nil {
		return nil, err
	}
	columns, err := structColumns(child, []types.Type{t.ItemType(), types.NewVoid()}, used)
	if err != nil {
		return nil, err
	}

	return func(row int) value.Value {
		start, end := offsets(row)
		items := make([]value.Value, int(end-start))
		for i := range items {
			items[i] = columns[0].value(int(start) + i)
		}

		return value.SetValueWithType(t, items)
	}, nil
}

func variantColumn[A Array](a A, t types.Type, fields []types.Type, active func(int) bool) (
	func(int) value.Value, error,
) {
	data, ok := any(a).(unionArray[A])
	if !ok || data.NumFields() != len(fields) {
		return nil, fmt.Errorf("expected Arrow union for YDB %s", t)
	}
	offset := func(row int) int { return row }
	if dense, ok := any(a).(interface{ ValueOffset(row int) int32 }); ok {
		offset = func(row int) int { return int(dense.ValueOffset(row)) }
	}
	used := make([][]bool, len(fields))
	for i := range fields {
		used[i] = make([]bool, data.Field(i).Len())
	}
	for row := 0; row < a.Len(); row++ {
		if !active(row) {
			continue
		}
		id, index := data.ChildID(row), offset(row)
		if id < 0 || id >= len(fields) || index < 0 || index >= len(used[id]) {
			return nil, fmt.Errorf("invalid Arrow variant at row %d", row)
		}
		used[id][index] = true
	}
	columns := make([]decodedColumn, len(fields))
	for i, field := range fields {
		var err error
		columns[i], err = newColumnActive(data.Field(i), field, func(row int) bool { return used[i][row] })
		if err != nil {
			return nil, err
		}
	}

	var wrap func(value.Value, int) value.Value
	switch t := t.(type) {
	case *types.VariantTuple:
		wrap = func(v value.Value, id int) value.Value { return value.VariantValueTuple(v, uint32(id), t) }
	case *types.VariantStruct:
		wrap = func(v value.Value, id int) value.Value { return value.VariantValueStruct(v, t.Field(id).Name, t) }
	default:
		return nil, fmt.Errorf("unsupported YDB variant type %s", t)
	}

	return func(row int) value.Value {
		id := data.ChildID(row)

		return wrap(columns[id].value(offset(row)), id)
	}, nil
}
