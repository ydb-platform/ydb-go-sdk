package witharrow

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

var _ query.ArrowDecoder = Decode

// Decode supports Bool, integers, Float, Double, String and Utf8, and a single
// Optional wrapper. Other YDB/Arrow types return an error; extend scalarReader
// for the types used by your queries.
func Decode(ctx context.Context, columns []query.ArrowColumn, part io.Reader) (batches []query.ArrowBatch, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	reader, err := ipc.NewReader(part)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	defer func() {
		if err != nil {
			for _, b := range batches {
				b.Release()
			}
			batches = nil
		}
	}()
	for reader.Next() {
		if err = ctx.Err(); err != nil {
			return batches, err
		}
		record := reader.RecordBatch()
		if int(record.NumCols()) != len(columns) {
			return batches, fmt.Errorf("arrow column count differs from YDB metadata")
		}
		readers := make([]directColumn, len(columns))
		for i, field := range record.Schema().Fields() {
			if field.Name != columns[i].Name {
				return batches, fmt.Errorf("arrow column %q differs from YDB column %q", field.Name, columns[i].Name)
			}
			readers[i], err = directColumnReader(record.Column(i), columns[i].Type)
			if err != nil {
				return batches, fmt.Errorf("column %q: %w", columns[i].Name, err)
			}
		}
		record.Retain()
		batches = append(batches, &batch{record: record, columns: readers})
	}
	return batches, reader.Err()
}

type directColumn struct {
	value func(int) types.Value
	scan  func(int, any) error
}

type batch struct {
	record  arrow.RecordBatch
	columns []directColumn
}

func (b *batch) NumRows() int                        { return int(b.record.NumRows()) }
func (b *batch) NumCols() int                        { return len(b.columns) }
func (b *batch) Scan(row, column int, dst any) error { return b.columns[column].scan(row, dst) }
func (b *batch) Value(row, column int) types.Value   { return b.columns[column].value(row) }
func (b *batch) Release() {
	if b.record != nil {
		b.record.Release()
		b.record = nil
		b.columns = nil
	}
}

func scanScalar[T any](a arrow.Array, optional bool, get func(int) T, clone func(T) T, fallback func(int) types.Value) func(int, any) error {
	return func(row int, dst any) error {
		switch ref := dst.(type) {
		case *T:
			if optional && a.IsNull(row) {
				return types.CastTo(fallback(row), dst)
			}
			v := get(row)
			if clone != nil {
				v = clone(v)
			}
			*ref = v
			return nil
		case **T:
			if !optional {
				return types.CastTo(fallback(row), dst)
			}
			if a.IsNull(row) {
				*ref = nil
				return nil
			}
			v := get(row)
			if clone != nil {
				v = clone(v)
			}
			*ref = &v
			return nil
		default:
			return types.CastTo(fallback(row), dst)
		}
	}
}

func directColumnReader(a arrow.Array, t types.Type) (directColumn, error) {
	read, err := columnReader(a, t)
	if err != nil {
		return directColumn{}, err
	}
	optional, inner := types.IsOptional(t)
	if !optional {
		inner = t
	}
	column := directColumn{value: read}
	switch data := a.(type) {
	case *array.Boolean:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.Int8:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.Int16:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.Int32:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.Int64:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.Uint8:
		if types.Equal(inner, types.TypeBool) {
			column.scan = scanScalar(data, optional, func(i int) bool { return data.Value(i) != 0 }, nil, read)
		} else {
			column.scan = scanScalar(data, optional, data.Value, nil, read)
		}
	case *array.Uint16:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.Uint32:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.Uint64:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.Float32:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.Float64:
		column.scan = scanScalar(data, optional, data.Value, nil, read)
	case *array.String:
		column.scan = scanScalar(data, optional, data.Value, strings.Clone, read)
	case *array.Binary:
		column.scan = scanScalar(data, optional, data.Value, bytes.Clone, read)
	}
	return column, nil
}

func columnReader(a arrow.Array, t types.Type) (func(int) types.Value, error) {
	inner := t
	optional := false
	if opt, ok := t.(interface {
		IsOptional()
		InnerType() types.Type
	}); ok {
		inner = opt.InnerType()
		optional = true
		if _, nested := inner.(interface{ IsOptional() }); nested {
			return nil, fmt.Errorf("nested Optional is unsupported")
		}
	}
	read, scalarType, err := scalarReader(a, inner)
	if err != nil {
		return nil, err
	}
	if !types.Equal(scalarType, inner) {
		return nil, fmt.Errorf("arrow %s does not match YDB %s", a.DataType(), inner)
	}
	if !optional {
		if a.NullN() != 0 {
			return nil, fmt.Errorf("null in non-optional %s", t)
		}
		return read, nil
	}
	scalar := read
	if types.Equal(inner, types.TypeBool) {
		trueValue := types.OptionalValue(types.BoolValue(true))
		falseValue := types.OptionalValue(types.BoolValue(false))
		read = func(i int) types.Value {
			if scalar(i) == types.BoolValue(true) {
				return trueValue
			}
			return falseValue
		}
	} else {
		read = func(i int) types.Value { return types.OptionalValue(scalar(i)) }
	}
	if a.NullN() == 0 {
		return read, nil
	}
	null := types.NullValue(inner)
	return func(i int) types.Value {
		if a.IsNull(i) {
			return null
		}
		return read(i)
	}, nil
}

func scalarReader(a arrow.Array, t types.Type) (func(int) types.Value, types.Type, error) {
	switch data := a.(type) {
	case *array.Boolean:
		return func(i int) types.Value { return types.BoolValue(data.Value(i)) }, types.TypeBool, nil
	case *array.Int8:
		return func(i int) types.Value { return types.Int8Value(data.Value(i)) }, types.TypeInt8, nil
	case *array.Int16:
		return func(i int) types.Value { return types.Int16Value(data.Value(i)) }, types.TypeInt16, nil
	case *array.Int32:
		return func(i int) types.Value { return types.Int32Value(data.Value(i)) }, types.TypeInt32, nil
	case *array.Int64:
		return func(i int) types.Value { return types.Int64Value(data.Value(i)) }, types.TypeInt64, nil
	case *array.Uint8:
		if types.Equal(t, types.TypeBool) {
			return func(i int) types.Value { return types.BoolValue(data.Value(i) != 0) }, types.TypeBool, nil
		}
		return func(i int) types.Value { return types.Uint8Value(data.Value(i)) }, types.TypeUint8, nil
	case *array.Uint16:
		return func(i int) types.Value { return types.Uint16Value(data.Value(i)) }, types.TypeUint16, nil
	case *array.Uint32:
		return func(i int) types.Value { return types.Uint32Value(data.Value(i)) }, types.TypeUint32, nil
	case *array.Uint64:
		return func(i int) types.Value { return types.Uint64Value(data.Value(i)) }, types.TypeUint64, nil
	case *array.Float32:
		return func(i int) types.Value { return types.FloatValue(data.Value(i)) }, types.TypeFloat, nil
	case *array.Float64:
		return func(i int) types.Value { return types.DoubleValue(data.Value(i)) }, types.TypeDouble, nil
	case *array.String:
		return func(i int) types.Value { return types.TextValue(strings.Clone(data.Value(i))) }, types.TypeText, nil
	case *array.Binary:
		return func(i int) types.Value { return types.BytesValue(bytes.Clone(data.Value(i))) }, types.TypeBytes, nil
	default:
		return nil, nil, fmt.Errorf("unsupported Arrow type %s", a.DataType())
	}
}
