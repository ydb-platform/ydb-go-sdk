package bind

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	errUnsupportedType         = errors.New("unsupported type")
	errUnnamedParam            = errors.New("unnamed param")
	errMultipleQueryParameters = errors.New("only one query arg *table.QueryParameters allowed")
)

//nolint:gocyclo
func toValue(v interface{}) (_ types.Value, err error) {
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

func toYdbParam(name string, value interface{}) (table.ParameterOption, error) {
	if na, ok := value.(driver.NamedValue); ok {
		n, v := na.Name, na.Value
		if n != "" {
			name = n
		}
		value = v
	}
	if na, ok := value.(sql.NamedArg); ok {
		n, v := na.Name, na.Value
		if n != "" {
			name = n
		}
		value = v
	}
	if v, ok := value.(table.ParameterOption); ok {
		return v, nil
	}
	v, err := toValue(value)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if name == "" {
		return nil, xerrors.WithStackTrace(errUnnamedParam)
	}
	if name[0] != '$' {
		name = "$" + name
	}
	return table.ValueParam(name, v), nil
}

func Params(args ...interface{}) (params []table.ParameterOption, _ error) {
	params = make([]table.ParameterOption, 0, len(args))
	for i, arg := range args {
		switch x := arg.(type) {
		case driver.NamedValue:
			if x.Name == "" {
				switch xx := x.Value.(type) {
				case *table.QueryParameters:
					if len(args) > 1 {
						return nil, xerrors.WithStackTrace(errMultipleQueryParameters)
					}
					xx.Each(func(name string, v types.Value) {
						params = append(params, table.ValueParam(name, v))
					})
				case table.ParameterOption:
					params = append(params, xx)
				default:
					x.Name = fmt.Sprintf("$p%d", i)
					param, err := toYdbParam(x.Name, x.Value)
					if err != nil {
						return nil, xerrors.WithStackTrace(err)
					}
					params = append(params, param)
				}
			} else {
				param, err := toYdbParam(x.Name, x.Value)
				if err != nil {
					return nil, xerrors.WithStackTrace(err)
				}
				params = append(params, param)
			}
		case sql.NamedArg:
			if x.Name == "" {
				return nil, xerrors.WithStackTrace(errUnnamedParam)
			}
			param, err := toYdbParam(x.Name, x.Value)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
			params = append(params, param)
		case *table.QueryParameters:
			if len(args) > 1 {
				return nil, xerrors.WithStackTrace(errMultipleQueryParameters)
			}
			x.Each(func(name string, v types.Value) {
				params = append(params, table.ValueParam(name, v))
			})
		case table.ParameterOption:
			params = append(params, x)
		default:
			param, err := toYdbParam(fmt.Sprintf("$p%d", i), x)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
			params = append(params, param)
		}
	}
	sort.Slice(params, func(i, j int) bool {
		return params[i].Name() < params[j].Name()
	})
	return params, nil
}
