package value

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

type tzDate32Value string

func (v tzDate32Value) castTo(dst any) error {
	return tzDateValue(v).castTo(dst)
}

func (v tzDate32Value) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), string(v))
}

func (tzDate32Value) Type() types.Type {
	return types.TzDate32
}

func (v tzDate32Value) toYDB() *Ydb.Value {
	return tzDateValue(v).toYDB()
}

type tzDatetime64Value string

func (v tzDatetime64Value) castTo(dst any) error {
	return tzDatetimeValue(v).castTo(dst)
}

func (v tzDatetime64Value) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), string(v))
}

func (tzDatetime64Value) Type() types.Type {
	return types.TzDatetime64
}

func (v tzDatetime64Value) toYDB() *Ydb.Value {
	return tzDatetimeValue(v).toYDB()
}

type tzTimestamp64Value string

func (v tzTimestamp64Value) castTo(dst any) error {
	return tzTimestampValue(v).castTo(dst)
}

func (v tzTimestamp64Value) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), string(v))
}

func (tzTimestamp64Value) Type() types.Type {
	return types.TzTimestamp64
}

func (v tzTimestamp64Value) toYDB() *Ydb.Value {
	return tzTimestampValue(v).toYDB()
}
