package xsql

import "github.com/ydb-platform/ydb-go-sdk/v3/table/types"

type valuer struct {
	v interface{}
}

func (p *valuer) UnmarshalYDB(raw types.RawValue) error {
	p.v = raw.Any()
	return nil
}

func (p *valuer) Value() interface{} {
	return p.v
}
