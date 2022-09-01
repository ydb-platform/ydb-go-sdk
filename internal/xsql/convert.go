package xsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func toQueryParams(values []driver.NamedValue) *table.QueryParameters {
	if len(values) == 0 {
		return nil
	}
	opts := make([]table.ParameterOption, len(values))
	for i, arg := range values {
		switch v := arg.Value.(type) {
		case types.Value:
			opts[i] = table.ValueParam(arg.Name, arg.Value.(types.Value))
		case table.ParameterOption:
			opts[i] = v
		case *table.QueryParameters:
			if len(values) != 1 {
				panic("only one arg with type *table.QueryParameters are supported")
			}
			return v
		default:
			panic(fmt.Sprintf("unsupported type: %T", v))
		}
	}
	return table.NewQueryParameters(opts...)
}

//nolint:gocyclo
func primitiveToValue(v interface{}) (value types.Value, err error) {
	if valuer, ok := v.(driver.Valuer); ok {
		v, err = valuer.Value()
		if err != nil {
			return nil, fmt.Errorf("ydb: driver.Valuer error: %w", err)
		}
	}

	switch x := v.(type) {
	case types.Value:
		return x, nil
	case bool:
		return types.BoolValue(x), nil
	case *bool:
		return types.NullableBoolValue(x), nil
	case int8:
		return types.Int8Value(x), nil
	case *int8:
		return types.NullableInt8Value(x), nil
	case uint8:
		return types.Uint8Value(x), nil
	case *uint8:
		return types.NullableUint8Value(x), nil
	case int16:
		return types.Int16Value(x), nil
	case *int16:
		return types.NullableInt16Value(x), nil
	case uint16:
		return types.Uint16Value(x), nil
	case *uint16:
		return types.NullableUint16Value(x), nil
	case int32:
		return types.Int32Value(x), nil
	case *int32:
		return types.NullableInt32Value(x), nil
	case uint32:
		return types.Uint32Value(x), nil
	case *uint32:
		return types.NullableUint32Value(x), nil
	case int64:
		return types.Int64Value(x), nil
	case *int64:
		return types.NullableInt64Value(x), nil
	case uint64:
		return types.Uint64Value(x), nil
	case *uint64:
		return types.NullableUint64Value(x), nil
	case float32:
		return types.FloatValue(x), nil
	case *float32:
		return types.NullableFloatValue(x), nil
	case float64:
		return types.DoubleValue(x), nil
	case *float64:
		return types.NullableDoubleValue(x), nil
	case []byte:
		return types.BytesValue(x), nil
	case *[]byte:
		return types.NullableBytesValue(x), nil
	case string:
		return types.TextValue(x), nil
	case *string:
		return types.NullableTextValue(x), nil
	case [16]byte:
		return types.UUIDValue(x), nil
	case *[16]byte:
		return types.NullableUUIDValue(x), nil
	case time.Time:
		return types.TimestampValueFromTime(x), nil
	case *time.Time:
		return types.NullableTimestampValueFromTime(x), nil
	case time.Duration:
		return types.IntervalValueFromDuration(x), nil
	case *time.Duration:
		return types.NullableIntervalValueFromDuration(x), nil
	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: unsupported type: %T", x))
	}
}

func checkNamedValue(v *driver.NamedValue) (err error) {
	if v.Name == "" {
		switch v.Value.(type) {
		case table.ParameterOption:
			return nil
		case *table.QueryParameters:
			return nil
		default:
			return xerrors.WithStackTrace(internal.ErrNameRequired)
		}
	}

	value, err := primitiveToValue(v.Value)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	v.Value = value

	return nil
}

// GenerateDeclareSection generates DECLARE section text in YQL query by params
//
// Warning: This is an experimental feature and could change at any time
func GenerateDeclareSection(args []sql.NamedArg) (string, error) {
	values := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		if arg.Name == "" {
			return "", xerrors.WithStackTrace(internal.ErrNameRequired)
		}
		value, err := primitiveToValue(arg.Value)
		if err != nil {
			return "", xerrors.WithStackTrace(err)
		}
		values[i] = table.ValueParam(arg.Name, value)
	}
	return internal.GenerateDeclareSection(table.NewQueryParameters(values...))
}
