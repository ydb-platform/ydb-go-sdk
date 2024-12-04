package bind

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	errUnsupportedType         = errors.New("unsupported type")
	errUnnamedParam            = errors.New("unnamed param")
	errMultipleQueryParameters = errors.New("only one query arg *table.QueryParameters allowed")
)

// Benchmarking of asUUID
//
// explicit UUID   	 2566760	       462.1 ns/op	      32 B/op	       2 allocs/op
// asUUID        	 2103366	       595.9 ns/op	      48 B/op	       3 allocs/op
func asUUID(v interface{}) (types.Value, bool) {
	if _, ok := v.(interface {
		URN() string
		ClockSequence() int
		ID() uint32
	}); !ok {
		return nil, false
	}

	switch vv := v.(type) {
	case uuid.UUID:
		return types.UuidValue(vv), true
	case *uuid.UUID:
		if vv == nil {
			return types.NullValue(types.TypeUUID), true
		}

		return types.OptionalValue(types.UuidValue(*vv)), true
	default:
		return nil, false
	}
}

func toType(v interface{}) (_ types.Type, err error) { //nolint:funlen
	switch x := v.(type) {
	case bool:
		return types.TypeBool, nil
	case int:
		return types.TypeInt32, nil
	case uint:
		return types.TypeUint32, nil
	case int8:
		return types.TypeInt8, nil
	case uint8:
		return types.TypeUint8, nil
	case int16:
		return types.TypeInt16, nil
	case uint16:
		return types.TypeUint16, nil
	case int32:
		return types.TypeInt32, nil
	case uint32:
		return types.TypeUint32, nil
	case int64:
		return types.TypeInt64, nil
	case uint64:
		return types.TypeUint64, nil
	case float32:
		return types.TypeFloat, nil
	case float64:
		return types.TypeDouble, nil
	case []byte:
		return types.TypeBytes, nil
	case string:
		return types.TypeText, nil
	case [16]byte:
		return nil, xerrors.Wrap(value.ErrIssue1501BadUUID)
	case time.Time:
		return types.TypeTimestamp, nil
	case time.Duration:
		return types.TypeInterval, nil
	default:
		kind := reflect.TypeOf(x).Kind()
		switch kind {
		case reflect.Slice, reflect.Array:
			v := reflect.ValueOf(x)
			t, err := toType(reflect.New(v.Type().Elem()).Elem().Interface())
			if err != nil {
				return nil, xerrors.WithStackTrace(
					fmt.Errorf("cannot parse slice item type %T: %w",
						x, errUnsupportedType,
					),
				)
			}

			return types.List(t), nil
		case reflect.Map:
			v := reflect.ValueOf(x)

			keyType, err := toType(reflect.New(v.Type().Key()).Interface())
			if err != nil {
				return nil, fmt.Errorf("cannot parse %T map key: %w",
					reflect.New(v.Type().Key()).Interface(), err,
				)
			}
			valueType, err := toType(reflect.New(v.Type().Elem()).Interface())
			if err != nil {
				return nil, fmt.Errorf("cannot parse %T map value: %w",
					v.MapKeys()[0].Interface(), err,
				)
			}

			return types.Dict(keyType, valueType), nil
		case reflect.Struct:
			v := reflect.ValueOf(x)

			fields := make([]types.StructOption, v.NumField())

			for i := range fields {
				kk, has := v.Type().Field(i).Tag.Lookup("sql")
				if !has {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %v as key field of struct: %w",
							v.Field(i).Interface(), errUnsupportedType,
						),
					)
				}
				tt, err := toType(v.Field(i).Interface())
				if err != nil {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %v as values of dict: %w",
							v.Field(i).Interface(), errUnsupportedType,
						),
					)
				}

				fields[i] = types.StructField(kk, tt)
			}

			return types.Struct(fields...), nil
		default:
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%T: %w. Create issue for support new type %s",
					x, errUnsupportedType, supportNewTypeLink(x),
				),
			)
		}
	}
}

//nolint:gocyclo,funlen
func toValue(v interface{}) (_ types.Value, err error) {
	if x, ok := asUUID(v); ok {
		if x == nil {
			return types.NullValue(types.TypeUUID), nil
		}

		return x, nil
	}

	switch x := v.(type) {
	case nil:
		return types.VoidValue(), nil
	case value.Value:
		return x, nil
	}

	if vv := reflect.ValueOf(v); vv.Kind() == reflect.Pointer {
		if vv.IsNil() {
			tt, err := toType(reflect.New(vv.Type().Elem()).Elem().Interface())
			if err != nil {
				return nil, xerrors.WithStackTrace(
					fmt.Errorf("cannot parse type of %T: %w",
						v, err,
					),
				)
			}

			return types.NullValue(tt), nil
		}

		vv, err := toValue(vv.Elem().Interface())
		if err != nil {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("cannot parse value of %T: %w",
					v, err,
				),
			)
		}

		return types.OptionalValue(vv), nil
	}

	switch x := v.(type) {
	case nil:
		return types.VoidValue(), nil
	case value.Value:
		return x, nil
	case bool:
		return types.BoolValue(x), nil
	case int:
		return types.Int32Value(int32(x)), nil
	case uint:
		return types.Uint32Value(uint32(x)), nil
	case int8:
		return types.Int8Value(x), nil
	case uint8:
		return types.Uint8Value(x), nil
	case int16:
		return types.Int16Value(x), nil
	case uint16:
		return types.Uint16Value(x), nil
	case int32:
		return types.Int32Value(x), nil
	case uint32:
		return types.Uint32Value(x), nil
	case int64:
		return types.Int64Value(x), nil
	case uint64:
		return types.Uint64Value(x), nil
	case float32:
		return types.FloatValue(x), nil
	case float64:
		return types.DoubleValue(x), nil
	case []byte:
		return types.BytesValue(x), nil
	case string:
		return types.TextValue(x), nil
	case []string:
		items := make([]types.Value, len(x))
		for i := range x {
			items[i] = types.TextValue(x[i])
		}

		return types.ListValue(items...), nil
	case [16]byte:
		return nil, xerrors.Wrap(value.ErrIssue1501BadUUID)
	case time.Time:
		return types.TimestampValueFromTime(x), nil
	case time.Duration:
		return types.IntervalValueFromDuration(x), nil
	default:
		kind := reflect.TypeOf(x).Kind()
		switch kind {
		case reflect.Slice, reflect.Array:
			v := reflect.ValueOf(x)
			list := make([]types.Value, v.Len())

			for i := range list {
				list[i], err = toValue(v.Index(i).Interface())
				if err != nil {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %d item of slice %T: %w",
							i, x, err,
						),
					)
				}
			}

			return value.ListValue(list...), nil
		case reflect.Map:
			v := reflect.ValueOf(x)
			fields := make([]types.DictValueOption, 0, len(v.MapKeys()))
			iter := v.MapRange()
			for iter.Next() {
				kk, err := toValue(iter.Key().Interface())
				if err != nil {
					return nil, fmt.Errorf("cannot parse %v map key: %w",
						iter.Key().Interface(), err,
					)
				}
				vv, err := toValue(iter.Value().Interface())
				if err != nil {
					return nil, fmt.Errorf("cannot parse %v map value: %w",
						iter.Value().Interface(), err,
					)
				}
				fields = append(fields, types.DictFieldValue(kk, vv))
			}

			return types.DictValue(fields...), nil
		case reflect.Struct:
			v := reflect.ValueOf(x)

			fields := make([]types.StructValueOption, v.NumField())

			for i := range fields {
				kk, has := v.Type().Field(i).Tag.Lookup("sql")
				if !has {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %v as key field of struct: %w",
							v.Field(i).Interface(), errUnsupportedType,
						),
					)
				}
				vv, err := toValue(v.Field(i).Interface())
				if err != nil {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %v as values of dict: %w",
							v.Index(i).Interface(), err,
						),
					)
				}

				fields[i] = types.StructFieldValue(kk, vv)
			}

			return types.StructValue(fields...), nil
		default:
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%T: %w. Create issue for support new type %s",
					x, errUnsupportedType, supportNewTypeLink(x),
				),
			)
		}
	}
}

func supportNewTypeLink(x interface{}) string {
	v := url.Values{}
	v.Add("labels", "enhancement,database/sql")
	v.Add("template", "02_FEATURE_REQUEST.md")
	v.Add("title", fmt.Sprintf("feat: Support new type `%T` in `database/sql` query args", x))

	return "https://github.com/ydb-platform/ydb-go-sdk/issues/new?" + v.Encode()
}

func toYdbParam(name string, value interface{}) (*params.Parameter, error) {
	if na, ok := value.(driver.NamedValue); ok {
		n, v := na.Name, na.Value
		if n != "" {
			name = n
		}
		value = v
	}
	if v, ok := value.(*params.Parameter); ok {
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

	return params.Named(name, v), nil
}

func Params(args ...interface{}) ([]*params.Parameter, error) {
	parameters := make([]*params.Parameter, 0, len(args))
	for i, arg := range args {
		var newParam *params.Parameter
		var newParams []*params.Parameter
		var err error
		switch x := arg.(type) {
		case driver.NamedValue:
			newParams, err = paramHandleNamedValue(x, i, len(args))
		case sql.NamedArg:
			if x.Name == "" {
				return nil, xerrors.WithStackTrace(errUnnamedParam)
			}
			newParam, err = toYdbParam(x.Name, x.Value)
			newParams = append(newParams, newParam)
		case *params.Params:
			if len(args) > 1 {
				return nil, xerrors.WithStackTrace(errMultipleQueryParameters)
			}
			parameters = *x
		case *params.Parameter:
			newParams = append(newParams, x)
		default:
			newParam, err = toYdbParam(fmt.Sprintf("$p%d", i), x)
			newParams = append(newParams, newParam)
		}
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		parameters = append(parameters, newParams...)
	}
	sort.Slice(parameters, func(i, j int) bool {
		return parameters[i].Name() < parameters[j].Name()
	})

	return parameters, nil
}

func paramHandleNamedValue(arg driver.NamedValue, paramNumber, argsLen int) ([]*params.Parameter, error) {
	if arg.Name == "" {
		switch x := arg.Value.(type) {
		case *params.Params:
			if argsLen > 1 {
				return nil, xerrors.WithStackTrace(errMultipleQueryParameters)
			}

			return *x, nil
		case *params.Parameter:
			return []*params.Parameter{x}, nil
		default:
			arg.Name = fmt.Sprintf("$p%d", paramNumber)
			param, err := toYdbParam(arg.Name, arg.Value)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return []*params.Parameter{param}, nil
		}
	} else {
		param, err := toYdbParam(arg.Name, arg.Value)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return []*params.Parameter{param}, nil
	}
}
