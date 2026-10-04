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
// Optional wrapper. Other YDB/Arrow types return an error; extend scalarValue
// for the types used by your queries.
func Decode(ctx context.Context, columns []query.ArrowColumn, part io.Reader) ([][]types.Value, error) {
	reader, err := ipc.NewReader(part)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	var rows [][]types.Value
	for reader.Next() {
		batch := reader.RecordBatch()
		if int(batch.NumCols()) != len(columns) {
			return nil, fmt.Errorf("arrow column count differs from YDB metadata")
		}
		for i, field := range batch.Schema().Fields() {
			if field.Name != columns[i].Name {
				return nil, fmt.Errorf("arrow column %q differs from YDB column %q", field.Name, columns[i].Name)
			}
		}
		count := int(batch.NumRows())
		start := len(rows)
		rows = append(rows, make([][]types.Value, count)...)
		for i := 0; i < count; i++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			row := make([]types.Value, len(columns))
			for j, column := range columns {
				row[j], err = columnValue(batch.Column(j), i, column.Type)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", column.Name, err)
				}
			}
			rows[start+i] = row
		}
	}
	return rows, reader.Err()
}

func columnValue(a arrow.Array, i int, t types.Type) (types.Value, error) {
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
	if a.IsNull(i) {
		if !optional {
			return nil, fmt.Errorf("null in non-optional %s", t)
		}
		return types.NullValue(inner), nil
	}
	v, err := scalarValue(a, i, inner)
	if err != nil {
		return nil, err
	}
	if !types.Equal(v.Type(), inner) {
		return nil, fmt.Errorf("arrow %s does not match YDB %s", a.DataType(), inner)
	}
	if optional {
		v = types.OptionalValue(v)
	}
	return v, nil
}

func scalarValue(a arrow.Array, i int, t types.Type) (types.Value, error) {
	switch data := a.(type) {
	case *array.Boolean:
		return types.BoolValue(data.Value(i)), nil
	case *array.Int8:
		return types.Int8Value(data.Value(i)), nil
	case *array.Int16:
		return types.Int16Value(data.Value(i)), nil
	case *array.Int32:
		return types.Int32Value(data.Value(i)), nil
	case *array.Int64:
		return types.Int64Value(data.Value(i)), nil
	case *array.Uint8:
		if types.Equal(t, types.TypeBool) {
			return types.BoolValue(data.Value(i) != 0), nil
		}
		return types.Uint8Value(data.Value(i)), nil
	case *array.Uint16:
		return types.Uint16Value(data.Value(i)), nil
	case *array.Uint32:
		return types.Uint32Value(data.Value(i)), nil
	case *array.Uint64:
		return types.Uint64Value(data.Value(i)), nil
	case *array.Float32:
		return types.FloatValue(data.Value(i)), nil
	case *array.Float64:
		return types.DoubleValue(data.Value(i)), nil
	case *array.String:
		return types.TextValue(strings.Clone(data.Value(i))), nil
	case *array.Binary:
		return types.BytesValue(bytes.Clone(data.Value(i))), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type %s", a.DataType())
	}
}
