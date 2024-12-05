package ydb

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type wrongParameters struct {
	err error
}

func (p wrongParameters) ToYDB(a *allocator.Allocator) (map[string]*Ydb.TypedValue, error) {
	return nil, xerrors.WithStackTrace(p.err)
}

// ParamsFromMap build parameters from named map
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func ParamsFromMap(m map[string]any) params.Parameters {
	namedParameters := make([]any, 0, len(m))
	for name, val := range m {
		namedParameters = append(namedParameters, driver.NamedValue{Name: name, Value: val})
	}
	p, err := bind.Params(namedParameters...)
	if err != nil {
		return wrongParameters{err: xerrors.WithStackTrace(err)}
	}

	return (*params.Params)(&p)
}
