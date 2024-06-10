//go:build !goexperiment.rangefunc

package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type (
	resultRange struct {
		r query.Result
	}
	resultSetRange struct {
		rs query.ResultSet
	}
)
