package bind

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type WideTypes struct{}

func (m WideTypes) ToYdb(sql string, args ...any) (yql string, newArgs []any, _ error) {
	newArgs = make([]any, 0, len(args))
	for _, arg := range args {
		switch t := arg.(type) {
		case time.Time:
			newArgs = append(newArgs, value.Timestamp64ValueFromTime(t))
		case time.Duration:
			newArgs = append(newArgs, value.Interval64ValueFromDuration(t))
		default:
			newArgs = append(newArgs, arg)
		}
	}

	return sql, newArgs, nil
}

func (m WideTypes) blockID() blockID {
	return blockCastArgs
}
