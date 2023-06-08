package xsql

import "github.com/ydb-platform/ydb-go-sdk/v3/table/types"

type valuer struct {
	v interface{}
}

func (v *valuer) UnmarshalYDB(raw types.RawValue) error {
	v.v = raw.Any()
	return nil
}

func (v *valuer) Value() interface{} {
	return v.v
}
