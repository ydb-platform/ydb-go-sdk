package xtable

import (
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type valuer struct {
	v interface{}
}

func (v *valuer) UnmarshalYDB(raw scanner.RawValue) error {
	v.v = raw.Any()

	return nil
}

func (v *valuer) Value() interface{} {
	// convert types to one of safe database sql types:
	// https://pkg.go.dev/database/sql/driver@go1.25.5#Value
	switch val := v.v.(type) {
	case uuid.UUID:
		return val.String()
	case value.UUIDIssue1501FixedBytesWrapper:
		return val.PublicRevertReorderForIssue1501().String()
	default:
		return val
	}
}
