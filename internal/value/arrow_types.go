package value

import (
	"database/sql/driver"
	"reflect"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func ListValueWithType(t types.Type, items []Value) Value {
	return &listValue{t: t, items: items}
}

func DictValueWithType(t types.Type, items []DictValueField) Value {
	v := DictValue(items...)
	v.t = t

	return v
}

func SetValueWithType(t types.Type, items []Value) Value {
	v := SetValue(items...)
	v.t = t

	return v
}

func TaggedValue(t *types.Tagged, inner Value) Value { return &taggedValue{t: t, value: inner} }
func TzDate32Value(text string) Value                { return tzDate32Value(text) }
func TzDatetime64Value(text string) Value            { return tzDatetime64Value(text) }
func TzTimestamp64Value(text string) Value           { return tzTimestamp64Value(text) }

type literalNullValue struct{}

func LiteralNullValue() Value              { return literalNullValue{} }
func (literalNullValue) Type() types.Type  { return types.NewNull() }
func (literalNullValue) Yql() string       { return "NULL" }
func (literalNullValue) toYDB() *Ydb.Value { return &Ydb.Value{Value: &Ydb.Value_NullFlagValue{}} }
func (literalNullValue) castTo(dst any) error {
	if ref, ok := dst.(*driver.Value); ok {
		*ref = nil

		return nil
	}
	ref := reflect.ValueOf(dst)
	if ref.Kind() != reflect.Pointer {
		return errDestinationTypeIsNotAPointer
	}
	ref.Elem().SetZero()

	return nil
}
