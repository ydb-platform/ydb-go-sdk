package query

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type PositionalArgsBind struct{}

func (m PositionalArgsBind) RewriteQuery(query string, args ...interface{}) (
	yql string, newArgs []interface{}, err error,
) {
	params, err := ToYdb(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	if len(params) == 0 {
		return query, args, nil
	}

	var (
		buffer = allocator.Buffers.Get()
		hits   = make(map[string]struct{}, len(args))
	)
	defer allocator.Buffers.Put(buffer)

	buffer.WriteString("-- origin query with positional args replacement\n")

	query, err = bindParams(query, paramTypePositional, func(paramName string) {
		hits[paramName] = struct{}{}
	})
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	if len(hits) > len(params) {
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: (positional args: %v, query args %d)", ErrInconsistentArgs, hits, len(params)),
		)
	}

	buffer.WriteString(query)

	for _, param := range params {
		newArgs = append(newArgs, param)
	}

	return buffer.String(), newArgs, nil
}
