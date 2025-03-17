package bind

import (
	"database/sql/driver"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type WideTimeTypes struct{}

func (m WideTimeTypes) ToYdb(sql string, args ...any) (yql string, newArgs []any, _ error) {
	newArgs = make([]any, 0, len(args))
	for _, arg := range args {
		switch t := arg.(type) {
		case driver.NamedValue:
			switch tt := t.Value.(type) {
			case time.Time:
				newArgs = append(newArgs, driver.NamedValue{
					Name:    t.Name,
					Ordinal: t.Ordinal,
					Value:   value.Timestamp64ValueFromTime(tt),
				})
			case time.Duration:
				newArgs = append(newArgs, driver.NamedValue{
					Name:    t.Name,
					Ordinal: t.Ordinal,
					Value:   value.Interval64ValueFromDuration(tt),
				})
			default:
				newArgs = append(newArgs, arg)
			}
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

func (m WideTimeTypes) blockID() blockID {
	return blockCastArgs
}
