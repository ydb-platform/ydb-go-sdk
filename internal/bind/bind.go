package bind

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type Bind interface {
	RewriteQuery(query string, args ...interface{}) (
		yql string, newArgs []interface{}, _ error,
	)
}

type Bindings []Bind

func (bindings Bindings) RewriteQuery(query string, args ...interface{}) (
	yql string, _ *table.QueryParameters, err error,
) {
	if len(bindings) == 0 {
		var params []table.ParameterOption
		params, err = Params(args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		return query, table.NewQueryParameters(params...), nil
	}

	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)

	for i := range bindings {
		query, args, err = bindings[len(bindings)-1-i].RewriteQuery(query, args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
	}

	params, err := Params(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	return query, table.NewQueryParameters(params...), nil
}
