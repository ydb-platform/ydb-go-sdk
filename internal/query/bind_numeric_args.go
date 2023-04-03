package query

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type NumericArgsBind struct{}

func (m NumericArgsBind) RewriteQuery(query string, args ...interface{}) (
	yql string, newArgs []interface{}, err error,
) {
	params, err := ToYdb(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	if len(params) == 0 {
		return query, args, nil
	}

	for _, param := range params {
		newArgs = append(newArgs, param)
	}

	var (
		buffer = allocator.Buffers.Get()
		hit    = make(map[string]struct{}, len(params))
		miss   = make(map[string]struct{}, len(params))
	)
	defer allocator.Buffers.Put(buffer)

	for _, param := range params {
		miss[param.Name()] = struct{}{}
	}

	buffer.WriteString("-- origin query with numeric args replacement\n")

	query, err = bindParams(query, paramTypeNumeric, func(paramName string) {
		hit[paramName] = struct{}{}
		delete(miss, paramName)
	})
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	if len(miss) > 0 {
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: %v", ErrInconsistentArgs, miss),
		)
	}

	if len(hit) != len(params) {
		for k := range miss {
			delete(miss, k)
		}
		for _, p := range params {
			if _, has := hit[p.Name()]; !has {
				miss[p.Name()] = struct{}{}
			}
		}
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: %v", ErrInconsistentArgs, miss),
		)
	}

	buffer.WriteString(query)

	return buffer.String(), newArgs, nil
}
