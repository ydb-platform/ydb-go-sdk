package value

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type tzDate32Value string

func (v tzDate32Value) castTo(dst any) error {
	return castWideTimezone(tzDateValue(v), string(v), LayoutTzDate, dst)
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
	return castWideTimezone(tzDatetimeValue(v), string(v), LayoutTzDatetime, dst)
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
	return castWideTimezone(tzTimestampValue(v), string(v), LayoutTzDatetime, dst)
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

func castWideTimezone(fallback Value, text, layout string, dst any) error {
	switch dst.(type) {
	case *time.Time, *driver.Value:
	default:
		return fallback.castTo(dst)
	}

	t, err := wideTimezoneToTime(text, layout)
	if err != nil {
		return err
	}
	switch v := dst.(type) {
	case *time.Time:
		*v = t
	case *driver.Value:
		*v = t
	}

	return nil
}

func wideTimezoneToTime(text, layout string) (time.Time, error) {
	date, zone, ok := strings.Cut(text, ",")
	if !ok {
		return time.Time{}, xerrors.WithStackTrace(fmt.Errorf("not found timezone location part in '%s'", text))
	}
	yearText, rest, ok := strings.Cut(strings.TrimPrefix(date, "-"), "-")
	if !ok {
		return time.Time{}, xerrors.WithStackTrace(fmt.Errorf("not found year part in '%s'", text))
	}
	year, err := strconv.Atoi(yearText)
	if err != nil {
		return time.Time{}, xerrors.WithStackTrace(fmt.Errorf("parse '%s' failed: %w", text, err))
	}
	if year == 0 {
		return time.Time{}, xerrors.WithStackTrace(fmt.Errorf("year zero is not supported in '%s'", text))
	}
	if strings.HasPrefix(date, "-") {
		// YQL has no year zero; Go uses astronomical year numbering.
		year = 1 - year
	}

	// Gregorian leap-year rules repeat every 400 years.
	parsed, err := time.Parse(layout, fmt.Sprintf("%04d-%s", 2000+year%400, rest))
	if err != nil {
		return time.Time{}, xerrors.WithStackTrace(fmt.Errorf("parse '%s' failed: %w", text, err))
	}
	location, err := time.LoadLocation(zone)
	if err != nil {
		return time.Time{}, xerrors.WithStackTrace(err)
	}

	return time.Date(year, parsed.Month(), parsed.Day(), parsed.Hour(), parsed.Minute(), parsed.Second(),
		parsed.Nanosecond(), location), nil
}
