package arrow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func NewDecoder[A Array, B Record[A], R IPCReader[B], O any](
	newReader func(io.Reader, ...O) (R, error), opts ...O,
) Decoder {
	opts = slices.Clone(opts)

	return func(ctx context.Context, columns []Column, part io.Reader) (batches []Batch, err error) {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		reader, err := newReader(part, opts...)
		if err != nil {
			return nil, err
		}
		defer reader.Release()
		defer func() {
			if err != nil {
				for _, batch := range batches {
					batch.Release()
				}
				batches = nil
			}
		}()
		for {
			if err = ctx.Err(); err != nil {
				return batches, err
			}
			record, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				return batches, nil
			}
			if readErr != nil {
				return batches, readErr
			}
			if record.NumCols() != int64(len(columns)) {
				return batches, fmt.Errorf("arrow column count differs from YDB metadata")
			}
			readers := make([]decodedColumn, len(columns))
			for i, column := range columns {
				if name := record.ColumnName(i); name != column.Name {
					return batches, fmt.Errorf("arrow column %q differs from YDB column %q", name, column.Name)
				}
				readers[i], err = newColumn(record.Column(i), column.Type)
				if err != nil {
					return batches, fmt.Errorf("column %q: %w", column.Name, err)
				}
			}
			record.Retain()
			batches = append(batches, &decodedBatch[A, B]{record: record, columns: readers})
		}
	}
}

type decodedColumn struct {
	value func(int) value.Value
	scan  func(int, any) error
}

type decodedBatch[A Array, B Record[A]] struct {
	record  B
	columns []decodedColumn
}

func (b *decodedBatch[A, B]) NumRows() int { return int(b.record.NumRows()) }
func (b *decodedBatch[A, B]) NumCols() int { return len(b.columns) }
func (b *decodedBatch[A, B]) Scan(row, column int, dst any) error {
	return b.columns[column].scan(row, dst)
}

func (b *decodedBatch[A, B]) Value(row, column int) value.Value {
	return b.columns[column].value(row)
}

func (b *decodedBatch[A, B]) Release() {
	if b.columns != nil {
		b.record.Release()
		var zero B
		b.record = zero
		b.columns = nil
	}
}

type scalar[T any] interface {
	Value(row int) T
}

//nolint:funlen // Scalar dispatch covers all supported Go types in one switch.
func newColumn(a Array, t types.Type) (decodedColumn, error) {
	inner := t
	optional := false
	if opt, ok := t.(types.Optional); ok {
		inner, optional = opt.InnerType(), true
	}
	if !optional && a.NullN() != 0 {
		return decodedColumn{}, fmt.Errorf("null in non-optional %s", t)
	}

	var column decodedColumn
	var scalarType types.Type
	switch data := a.(type) {
	case scalar[bool]:
		scalarType = types.Bool
		column = newScalarColumn(a, inner, optional, data.Value, value.BoolValue, nil)
	case scalar[int8]:
		scalarType = types.Int8
		column = newScalarColumn(a, inner, optional, data.Value, value.Int8Value, nil)
	case scalar[int16]:
		scalarType = types.Int16
		column = newScalarColumn(a, inner, optional, data.Value, value.Int16Value, nil)
	case scalar[int32]:
		scalarType = types.Int32
		column = newScalarColumn(a, inner, optional, data.Value, value.Int32Value, nil)
	case scalar[int64]:
		scalarType = types.Int64
		column = newScalarColumn(a, inner, optional, data.Value, value.Int64Value, nil)
	case scalar[uint8]:
		if types.Equal(inner, types.Bool) {
			scalarType = types.Bool
			column = newScalarColumn(a, inner, optional, func(i int) bool { return data.Value(i) != 0 }, value.BoolValue, nil)
		} else {
			scalarType = types.Uint8
			column = newScalarColumn(a, inner, optional, data.Value, value.Uint8Value, nil)
		}
	case scalar[uint16]:
		scalarType = types.Uint16
		column = newScalarColumn(a, inner, optional, data.Value, value.Uint16Value, nil)
	case scalar[uint32]:
		scalarType = types.Uint32
		column = newScalarColumn(a, inner, optional, data.Value, value.Uint32Value, nil)
	case scalar[uint64]:
		scalarType = types.Uint64
		column = newScalarColumn(a, inner, optional, data.Value, value.Uint64Value, nil)
	case scalar[float32]:
		scalarType = types.Float
		column = newScalarColumn(a, inner, optional, data.Value, value.FloatValue, nil)
	case scalar[float64]:
		scalarType = types.Double
		column = newScalarColumn(a, inner, optional, data.Value, value.DoubleValue, nil)
	case scalar[string]:
		scalarType = types.Text
		column = newScalarColumn(a, inner, optional, data.Value, value.TextValue, strings.Clone)
	case scalar[[]byte]:
		scalarType = types.Bytes
		column = newScalarColumn(a, inner, optional, data.Value, value.BytesValue, bytes.Clone)
	default:
		return decodedColumn{}, fmt.Errorf("unsupported Arrow array %T for YDB %s", a, t)
	}
	if !types.Equal(scalarType, inner) {
		return decodedColumn{}, fmt.Errorf("arrow array %T does not match YDB %s", a, inner)
	}

	return column, nil
}

func newScalarColumn[T any, V value.Value](
	a Array, inner types.Type, optional bool, get func(int) T, makeValue func(T) V, clone func(T) T,
) decodedColumn {
	read := scalarValues(a, inner, optional, get, makeValue, clone)

	return decodedColumn{value: read, scan: scanScalar(a, optional, get, clone, read)}
}

func scalarValues[T any, V value.Value](
	a Array, inner types.Type, optional bool, get func(int) T, makeValue func(T) V, clone func(T) T,
) func(int) value.Value {
	read := func(row int) value.Value {
		v := get(row)
		if clone != nil {
			v = clone(v)
		}

		return makeValue(v)
	}
	if !optional {
		return read
	}
	scalar := read
	if types.Equal(inner, types.Bool) {
		trueValue := value.OptionalValue(value.BoolValue(true))
		falseValue := value.OptionalValue(value.BoolValue(false))
		read = func(row int) value.Value {
			if scalar(row) == value.BoolValue(true) {
				return trueValue
			}

			return falseValue
		}
	} else {
		read = func(row int) value.Value { return value.OptionalValue(scalar(row)) }
	}
	if a.NullN() == 0 {
		return read
	}
	nonNull := read
	null := value.NullValue(inner)

	return func(row int) value.Value {
		if a.IsNull(row) {
			return null
		}

		return nonNull(row)
	}
}

func scanScalar[T any](
	a Array, optional bool, get func(int) T, clone func(T) T, fallback func(int) value.Value,
) func(int, any) error {
	return func(row int, dst any) error {
		switch ref := dst.(type) {
		case *T:
			if optional && a.IsNull(row) {
				return value.CastTo(fallback(row), dst)
			}
			v := get(row)
			if clone != nil {
				v = clone(v)
			}
			*ref = v

			return nil
		case **T:
			if !optional {
				return value.CastTo(fallback(row), dst)
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
			return value.CastTo(fallback(row), dst)
		}
	}
}
