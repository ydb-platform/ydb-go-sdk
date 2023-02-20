package bind

import "errors"

var (
	ErrMultipleQueryParameters = errors.New("only one query arg *table.QueryParameters allowed")
	errUnknownYdbParam         = errors.New("unknown ydb param")
	errUnknownQueryType        = errors.New("unknown query type (mixed args type or nothing args)")

	errUnsupportedType = errors.New("unsupported type")
)
