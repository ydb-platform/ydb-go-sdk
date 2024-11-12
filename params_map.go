package ydb

import (
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
)

// MustParamsFromMap build parameters from named map, panic if error
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func MustParamsFromMap(m map[string]any) *params.Parameters {
	p, err := ParamsFromMap(m)
	if err != nil {
		panic(fmt.Sprintf("ydb: MustParamsFromMap failed with error: %v", err))
	}

	return p
}

// ParamsFromMap build parameters from named map
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func ParamsFromMap(m map[string]any) (*params.Parameters, error) {
	namedParameters := make([]any, 0, len(m))
	for name, val := range m {
		namedParameters = append(namedParameters, driver.NamedValue{Name: name, Value: val})
	}
	p, err := bind.Params(namedParameters...)

	return (*params.Parameters)(&p), err
}
