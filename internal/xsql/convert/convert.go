package convert

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	errUnsupportedType = errors.New("unsupported type")
	errUnnamedParam    = errors.New("unnamed param")
)

//nolint:gocyclo
func ToValue(v interface{}) (_ types.Value, err error) {
	if valuer, ok := v.(driver.Valuer); ok {
		v, err = valuer.Value()
		if err != nil {
			return nil, fmt.Errorf("ydb: driver.Valuer error: %w", err)
		}
	}

	switch x := v.(type) {
	case nil:
		return types.VoidValue(), nil
	case value.Value:
		return x, nil
	case bool:
		return types.BoolValue(x), nil
	case *bool:
		return types.NullableBoolValue(x), nil
	case int:
		return types.Int32Value(int32(x)), nil
	case *int:
		if x == nil {
			return types.NullValue(types.TypeInt32), nil
		}
		xx := int32(*x)
		return types.NullableInt32Value(&xx), nil
	case uint:
		return types.Uint32Value(uint32(x)), nil
	case *uint:
		if x == nil {
			return types.NullValue(types.TypeUint32), nil
		}
		xx := uint32(*x)
		return types.NullableUint32Value(&xx), nil
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
	case []string:
		items := make([]types.Value, len(x))
		for i := range x {
			items[i] = types.TextValue(x[i])
		}
		return types.ListValue(items...), nil
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
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("%T: %w. Create issue for support new type %s",
				x, errUnsupportedType, supportNewTypeLink(x),
			),
		)
	}
}

func supportNewTypeLink(x interface{}) string {
	v := url.Values{}
	v.Add("labels", "enhancement,database/sql")
	v.Add("template", "02_FEATURE_REQUEST.md")
	v.Add("title", fmt.Sprintf("feat: Support new type `%T` in `database/sql` query args", x))
	return "https://github.com/ydb-platform/ydb-go-sdk/issues/new?" + v.Encode()
}

func ToYdbParam(param driver.NamedValue) (table.ParameterOption, error) {
	if v, ok := param.Value.(table.ParameterOption); ok {
		return v, nil
	}
	value, err := ToValue(param.Value)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if param.Name == "" {
		return nil, xerrors.WithStackTrace(errUnnamedParam)
	}
	return table.ValueParam(param.Name, value), nil
}
