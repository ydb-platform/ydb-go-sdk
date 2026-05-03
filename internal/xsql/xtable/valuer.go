package xtable

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
)

type valuer struct {
	v any
}

func (v *valuer) UnmarshalYDB(raw scanner.RawValue) error {
	v.v = raw.Any()

	return nil
}

func (v *valuer) Value() any {
	return common.ToDatabaseSQLValue(v.v)
}
