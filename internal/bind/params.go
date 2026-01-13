package bind

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

var (
	errUnsupportedType         = errors.New("unsupported type")
	errUnnamedParam            = errors.New("unnamed param")
	errMultipleQueryParameters = errors.New("only one query arg *table.QueryParameters allowed")
)

var (
	uuidType    = reflect.TypeOf(uuid.UUID{})
	uuidPtrType = reflect.TypeOf((*uuid.UUID)(nil))
)

func asUUID(v any) (value.Value, bool) {
	switch reflect.TypeOf(v) {
	case uuidType:
		return value.Uuid(v.(uuid.UUID)), true //nolint:forcetypeassert
	case uuidPtrType:
		vv := v.(*uuid.UUID) //nolint:forcetypeassert
		if vv == nil {
			return value.NullValue(types.UUID), true
		}

		return value.OptionalValue(value.Uuid(*(v.(*uuid.UUID)))), true //nolint:forcetypeassert
	}

	return nil, false
}

func asSQLNull(v any) (value.Value, bool) {
	switch x := v.(type) {
	case sql.NullBool:
		return wrapWithNulls(x.Valid, value.BoolValue(x.Bool), types.Bool), true
	case sql.NullFloat64:
		return wrapWithNulls(x.Valid, value.DoubleValue(x.Float64), types.Double), true
	case sql.NullInt16:
		return wrapWithNulls(x.Valid, value.Int16Value(x.Int16), types.Int16), true
	case sql.NullInt32:
		return wrapWithNulls(x.Valid, value.Int32Value(x.Int32), types.Int32), true
	case sql.NullInt64:
		return wrapWithNulls(x.Valid, value.Int64Value(x.Int64), types.Int64), true
	case sql.NullString:
		return wrapWithNulls(x.Valid, value.TextValue(x.String), types.Text), true
	case sql.NullTime:
		return wrapWithNulls(x.Valid, value.TimestampValueFromTime(x.Time), types.Timestamp), true
	}

	return asSQLNullGeneric(v)
}

func wrapWithNulls(valid bool, val value.Value, t types.Type) value.Value {
	if valid {
		return value.OptionalValue(val)
	}

	return value.NullValue(t)
}

func asSQLNullGeneric(v any) (value.Value, bool) {
	if v == nil {
		return nil, false
	}

	rv := reflect.ValueOf(v)
	rt := rv.Type()

	if rv.Kind() != reflect.Struct {
		return nil, false
	}

	vField := rv.FieldByName("V")
	validField := rv.FieldByName("Valid")

	if !vField.IsValid() || !validField.IsValid() {
		return nil, false
	}

	if validField.Kind() != reflect.Bool {
		return nil, false
	}

	if !strings.HasPrefix(rt.String(), "sql.Null[") {
		return nil, false
	}

	valid := validField.Bool()
	if !valid {
		nullType, err := toType(vField.Interface())
		if err != nil {
			return value.NullValue(types.Text), true
		}

		return value.NullValue(nullType), true
	}

	return asSQLNullValue(vField.Interface())
}

func asSQLNullValue(v any) (value.Value, bool) {
	val, err := toValue(v)
	if err != nil {
		return nil, false
	}

	return value.OptionalValue(val), true
}

// isDurationTypeAlias checks if the given type is time.Duration or its type alias.
func isDurationTypeAlias(rt reflect.Type) bool {
	if rt.Kind() != reflect.Int64 {
		return false
	}
	durationType := reflect.TypeFor[time.Duration]()
	int64Type := reflect.TypeFor[int64]()
	// Check if it's a named type (not just int64) and convertible to time.Duration
	if rt == int64Type || !rt.ConvertibleTo(durationType) || !durationType.ConvertibleTo(rt) {
		return false
	}
	// Check if the type name suggests it's a duration type
	typeName := rt.Name()

	return typeName != "" && strings.Contains(typeName, "Duration")
}

// tryConvertToBaseType attempts to convert a value to a base type and returns the converted value if successful.
// Returns nil if conversion is not possible or not needed.
func tryConvertToBaseType(rv reflect.Value) any {
	rt := rv.Type()
	// Special handling for time.Duration - check first to avoid converting to int64
	if isDurationTypeAlias(rt) {
		durationType := reflect.TypeFor[time.Duration]()
		if rv.CanConvert(durationType) {
			return rv.Convert(durationType).Interface()
		}
	}

	kind := rt.Kind()
	baseTypes := map[reflect.Kind]reflect.Type{
		reflect.String:  reflect.TypeFor[string](),
		reflect.Int:     reflect.TypeFor[int](),
		reflect.Int8:    reflect.TypeFor[int8](),
		reflect.Int16:   reflect.TypeFor[int16](),
		reflect.Int32:   reflect.TypeFor[int32](),
		reflect.Int64:   reflect.TypeFor[int64](),
		reflect.Uint:    reflect.TypeFor[uint](),
		reflect.Uint8:   reflect.TypeFor[uint8](),
		reflect.Uint16:  reflect.TypeFor[uint16](),
		reflect.Uint32:  reflect.TypeFor[uint32](),
		reflect.Uint64:  reflect.TypeFor[uint64](),
		reflect.Float32: reflect.TypeFor[float32](),
		reflect.Float64: reflect.TypeFor[float64](),
		reflect.Bool:    reflect.TypeFor[bool](),
	}

	baseType, ok := baseTypes[kind]
	if !ok {
		return nil
	}

	if !rv.CanConvert(baseType) {
		return nil
	}

	return rv.Convert(baseType).Interface()
}

//nolint:funlen
func toType(v any) (_ types.Type, err error) {
	switch x := v.(type) {
	case reflect.Value:
		return toType(x.Interface())
	case bool:
		return types.Bool, nil
	case int:
		return types.Int32, nil
	case uint:
		return types.Uint32, nil
	case int8:
		return types.Int8, nil
	case uint8:
		return types.Uint8, nil
	case int16:
		return types.Int16, nil
	case uint16:
		return types.Uint16, nil
	case int32:
		return types.Int32, nil
	case uint32:
		return types.Uint32, nil
	case int64:
		return types.Int64, nil
	case uint64:
		return types.Uint64, nil
	case float32:
		return types.Float, nil
	case float64:
		return types.Double, nil
	case []byte:
		return types.Bytes, nil
	case string:
		return types.Text, nil
	case [16]byte:
		return nil, xerrors.Wrap(value.ErrIssue1501BadUUID)
	case time.Time:
		return types.Timestamp, nil
	case time.Duration:
		return types.Interval, nil
	default:
		rv := reflect.ValueOf(x)
		// Try to convert derived types to base types
		if converted := tryConvertToBaseType(rv); converted != nil {
			return toType(converted)
		}

		return reflectKindToType(x)
	}
}

func reflectKindToType(x any) (types.Type, error) { //nolint:funlen
	rt := reflect.TypeOf(x)
	kind := rt.Kind()
	switch kind {
	case reflect.String:
		return types.Text, nil
	case reflect.Int:
		return types.Int32, nil
	case reflect.Int8:
		return types.Int8, nil
	case reflect.Int16:
		return types.Int16, nil
	case reflect.Int32:
		return types.Int32, nil
	case reflect.Int64:
		if isDurationTypeAlias(rt) {
			return types.Interval, nil
		}

		return types.Int64, nil
	case reflect.Uint:
		return types.Uint32, nil
	case reflect.Uint8:
		return types.Uint8, nil
	case reflect.Uint16:
		return types.Uint16, nil
	case reflect.Uint32:
		return types.Uint32, nil
	case reflect.Uint64:
		return types.Uint64, nil
	case reflect.Float32:
		return types.Float, nil
	case reflect.Float64:
		return types.Double, nil
	case reflect.Bool:
		return types.Bool, nil
	case reflect.Slice, reflect.Array:
		v := reflect.ValueOf(x)
		// Special handling for byte slices ([]byte and type aliases)
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return types.Bytes, nil
		}
		t, err := toType(reflect.New(v.Type().Elem()).Elem().Interface())
		if err != nil {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("cannot parse slice item type %T: %w",
					x, errUnsupportedType,
				),
			)
		}

		return types.NewList(t), nil
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

		return types.NewDict(keyType, valueType), nil
	case reflect.Struct:
		v := reflect.ValueOf(x)

		fields := make([]types.StructField, v.NumField())

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
					fmt.Errorf("cannot parse %v as values of struct: %w",
						v.Field(i).Interface(), errUnsupportedType,
					),
				)
			}

			fields[i] = types.StructField{
				Name: kk,
				T:    tt,
			}
		}

		return types.NewStruct(fields...), nil
	default:
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("%T: %w. Create issue for support new type %s",
				x, errUnsupportedType, supportNewTypeLink(x),
			),
		)
	}
}

//nolint:gocyclo,funlen
func toValue(v any) (_ value.Value, err error) {
	if x, ok := asUUID(v); ok {
		return x, nil
	}

	if nullValue, ok := asSQLNull(v); ok {
		return nullValue, nil
	}

	if valuer, ok := v.(driver.Valuer); ok {
		v, err = valuer.Value()
		if err != nil {
			return nil, xerrors.WithStackTrace(fmt.Errorf("driver.Valuer error: %w", err))
		}
	}

	if x, ok := asUUID(v); ok {
		return x, nil
	}

	switch x := v.(type) {
	case nil:
		return value.VoidValue(), nil
	case reflect.Value:
		return toValue(x.Interface())
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

			return value.NullValue(tt), nil
		}

		vv, err := toValue(vv.Elem().Interface())
		if err != nil {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("cannot parse value of %T: %w",
					v, err,
				),
			)
		}

		return value.OptionalValue(vv), nil
	}

	switch x := v.(type) {
	case reflect.Value:
		return toValue(x.Interface())
	case value.Value:
		return x, nil
	case bool:
		return value.BoolValue(x), nil
	case int:
		return value.Int32Value(int32(x)), nil
	case uint:
		return value.Uint32Value(uint32(x)), nil
	case int8:
		return value.Int8Value(x), nil
	case uint8:
		return value.Uint8Value(x), nil
	case int16:
		return value.Int16Value(x), nil
	case uint16:
		return value.Uint16Value(x), nil
	case int32:
		return value.Int32Value(x), nil
	case uint32:
		return value.Uint32Value(x), nil
	case int64:
		return value.Int64Value(x), nil
	case uint64:
		return value.Uint64Value(x), nil
	case float32:
		return value.FloatValue(x), nil
	case float64:
		return value.DoubleValue(x), nil
	case []byte:
		return value.BytesValue(x), nil
	case string:
		return value.TextValue(x), nil
	case []string:
		items := make([]value.Value, len(x))
		for i := range x {
			items[i] = value.TextValue(x[i])
		}

		return value.ListValue(items...), nil
	case value.UUIDIssue1501FixedBytesWrapper:
		return value.UUIDWithIssue1501Value(x.AsBytesArray()), nil
	case [16]byte:
		return nil, xerrors.Wrap(value.ErrIssue1501BadUUID)
	case time.Time:
		return value.TimestampValueFromTime(x), nil
	case time.Duration:
		return value.IntervalValueFromDuration(x), nil
	case json.Marshaler:
		bytes, err := x.MarshalJSON()
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return value.JSONValue(xstring.FromBytes(bytes)), nil
	default:
		rv := reflect.ValueOf(x)
		// Try to convert derived types to base types
		if converted := tryConvertToBaseType(rv); converted != nil {
			return toValue(converted)
		}

		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			v := reflect.ValueOf(x)
			// Special handling for byte slices ([]byte and type aliases)
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return value.BytesValue(v.Bytes()), nil
			}
			list := make([]value.Value, v.Len())

			for i := range list {
				list[i], err = toValue(v.Index(i).Interface())
				if err != nil {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse item #%d of slice %T: %w",
							i, x, err,
						),
					)
				}
			}

			return value.ListValue(list...), nil
		case reflect.Map:
			v := reflect.ValueOf(x)
			fields := make([]value.DictValueField, 0, len(v.MapKeys()))
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
				fields = append(fields, value.DictValueField{
					K: kk,
					V: vv,
				})
			}

			return value.DictValue(fields...), nil
		case reflect.Struct:
			v := reflect.ValueOf(x)

			fields := make([]value.StructValueField, v.NumField())

			for i := range fields {
				kk, has := v.Type().Field(i).Tag.Lookup("sql")
				if !has {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %q as key field of struct %T: %w",
							v.Type().Field(i).Name, x, errUnsupportedType,
						),
					)
				}
				vv, err := toValue(v.Field(i).Interface())
				if err != nil {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %v as values of struct: %w",
							v.Field(i).Interface(), err,
						),
					)
				}

				fields[i] = value.StructValueField{
					Name: kk,
					V:    vv,
				}
			}

			return value.StructValue(fields...), nil
		default:
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%T: %w. Create issue for support new type %s",
					x, errUnsupportedType, supportNewTypeLink(x),
				),
			)
		}
	}
}

func supportNewTypeLink(x any) string {
	v := url.Values{}
	v.Add("labels", "enhancement,database/sql")
	v.Add("template", "02_FEATURE_REQUEST.md")
	v.Add("title", fmt.Sprintf("feat: Support new type `%T` in `database/sql` query args", x))

	return "https://github.com/ydb-platform/ydb-go-sdk/issues/new?" + v.Encode()
}

func toYdbParam(name string, value any) (*params.Parameter, error) {
	if nv, has := value.(driver.NamedValue); has {
		n, v := nv.Name, nv.Value
		if n != "" {
			name = n
		}
		value = v
	}

	if nv, ok := value.(params.NamedValue); ok {
		return params.Named(nv.Name(), nv.Value()), nil
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

func Params(args ...any) ([]*params.Parameter, error) {
	parameters := make([]*params.Parameter, 0, len(args))
	for i, arg := range args {
		var (
			newParam  *params.Parameter
			newParams []*params.Parameter
			err       error
		)
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
