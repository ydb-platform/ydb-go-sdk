// nolint:revive
package ydb_table_result_named

type Type uint8

const (
	TypeUnknown Type = iota
	TypeRequired
	TypeOptional
	TypeOptionalWithUseDefault
)

type Value struct {
	Name  string
	Value interface{}
	Type  Type
}

// Optional returns Value with key as column name and value as destination
// Warning: value must double-pointed data destination
func Optional(key string, value interface{}) Value {
	if key == "" {
		panic("key must be not empty")
	}
	return Value{
		Name:  key,
		Value: value,
		Type:  TypeOptional,
	}
}

// Required returns Value with key as column name and value as destination
// Warning: value must single-pointed data destination
func Required(key string, value interface{}) Value {
	if key == "" {
		panic("key must be not empty")
	}
	return Value{
		Name:  key,
		Value: value,
		Type:  TypeRequired,
	}
}

// OptionalWithDefault returns Value with key as column name and value as destination
// If scanned YDB value is NULL - default type value will be applied to value destination
// Warning: value must single-pointed data destination
func OptionalWithDefault(key string, value interface{}) Value {
	if key == "" {
		panic("key must be not empty")
	}
	return Value{
		Name:  key,
		Value: value,
		Type:  TypeOptionalWithUseDefault,
	}
}
