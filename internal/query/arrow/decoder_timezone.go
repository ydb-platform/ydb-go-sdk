package arrow

import (
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func timezoneType(t types.Primitive) bool {
	return t == types.TzDate || t == types.TzDatetime || t == types.TzTimestamp ||
		t == types.TzDate32 || t == types.TzDatetime64 || t == types.TzTimestamp64
}

//nolint:funlen,gocyclo // Each timezone type has a distinct physical width and calendar format.
func timezoneColumn[A Array](a A, t types.Primitive, active func(int) bool) (func(int) value.Value, error) {
	data, ok := any(a).(structArray[A])
	if !ok || data.NumField() != 2 {
		return nil, fmt.Errorf("expected Arrow timezone struct for YDB %s", t)
	}
	zones, ok := any(data.Field(1)).(scalar[string])
	if !ok {
		return nil, fmt.Errorf("expected Arrow timezone names")
	}
	locations := make(map[string]*time.Location)
	for row := 0; row < a.Len(); row++ {
		if !active(row) {
			continue
		}
		zone := zones.Value(row)
		if _, exists := locations[zone]; !exists {
			var err error
			locations[zone], err = time.LoadLocation(zone)
			if err != nil {
				return nil, err
			}
		}
	}
	physical := data.Field(0)
	var get func(int) time.Time
	var layout string
	switch t {
	case types.TzDate:
		if days, ok := any(physical).(scalar[uint16]); ok {
			get = func(row int) time.Time { return time.Unix((int64(days.Value(row))+1)*86400-1, 0) }
		}
		layout = value.LayoutTzDate
	case types.TzDate32:
		if days, ok := any(physical).(scalar[int32]); ok {
			get = func(row int) time.Time { return time.Unix((int64(days.Value(row))+1)*86400-1, 0) }
		}
		layout = value.LayoutTzDate
	case types.TzDatetime:
		if seconds, ok := any(physical).(scalar[uint32]); ok {
			get = func(row int) time.Time { return time.Unix(int64(seconds.Value(row)), 0) }
		}
		layout = value.LayoutTzDatetime
	case types.TzDatetime64:
		if seconds, ok := any(physical).(scalar[int64]); ok {
			get = func(row int) time.Time { return time.Unix(seconds.Value(row), 0) }
		}
		layout = value.LayoutTzDatetime
	case types.TzTimestamp:
		if micros, ok := any(physical).(scalar[uint64]); ok {
			get = func(row int) time.Time { return time.UnixMicro(int64(micros.Value(row))) }
		}
		layout = value.LayoutTzTimestamp
	case types.TzTimestamp64:
		if micros, ok := any(physical).(scalar[int64]); ok {
			get = func(row int) time.Time { return time.UnixMicro(micros.Value(row)) }
		}
		layout = value.LayoutTzTimestamp
	}
	if get == nil {
		return nil, fmt.Errorf("arrow timezone datetime does not match YDB %s", t)
	}

	return func(row int) value.Value {
		zone := zones.Value(row)
		date := get(row).In(locations[zone])
		text := date.Format(layout)
		if t == types.TzDate32 || t == types.TzDatetime64 || t == types.TzTimestamp64 {
			year := date.Year()
			if year <= 0 {
				year--
			}
			text = fmt.Sprintf("%d-%02d-%02d", year, date.Month(), date.Day())
			switch t {
			case types.TzDatetime64:
				text += date.Format("T15:04:05")
			case types.TzTimestamp64:
				text += date.Format("T15:04:05.999999")
			}
		}
		text += "," + zone
		switch t {
		case types.TzDate:
			return value.TzDateValue(text)
		case types.TzDatetime:
			return value.TzDatetimeValue(text)
		case types.TzTimestamp:
			return value.TzTimestampValue(text)
		case types.TzDate32:
			return value.TzDate32Value(text)
		case types.TzDatetime64:
			return value.TzDatetime64Value(text)
		default:
			return value.TzTimestamp64Value(text)
		}
	}, nil
}
