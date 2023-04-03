package query

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type AutoDeclareBind struct{}

func (m AutoDeclareBind) RewriteQuery(query string, args ...interface{}) (
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
		declares = make([]string, 0, len(params))
		buffer   = allocator.Buffers.Get()
	)
	defer allocator.Buffers.Put(buffer)

	buffer.WriteString("-- bind declares\n")

	for _, param := range params {
		declares = append(declares, "DECLARE "+param.Name()+" AS "+param.Value().Type().Yql()+";")
	}

	sort.Strings(declares)

	for _, d := range declares {
		buffer.WriteString(d)
		buffer.WriteByte('\n')
	}

	buffer.WriteByte('\n')

	buffer.WriteString(query)

	for _, param := range params {
		newArgs = append(newArgs, param)
	}

	return buffer.String(), newArgs, nil
}
