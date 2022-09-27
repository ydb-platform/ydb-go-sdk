package value

import (
	"math"
	"time"
)

const InfiniteDuration = time.Duration(math.MaxInt64)

const (
	secondsPerMinute uint64 = 60
	secondsPerHour          = 60 * secondsPerMinute
	secondsPerDay           = 24 * secondsPerHour
)

// Date format layouts described in time.Format and time.ANSIC docs.
const (
	LayoutDate        = "2006-01-02"
	LayoutDatetime    = "2006-01-02 15:04:05"
	LayoutTimestamp   = "2006-01-02 15:04:05.000000"
	LayoutTzDate      = "2006-01-02,MST"
	LayoutTzDatetime  = "2006-01-02T15:04:05,MST"
	LayoutTzTimestamp = "2006-01-02T15:04:05.000000,MST"
)

var unix = time.Unix(0, 0)

// IntervalToDuration returns time.Duration from given microseconds
func IntervalToDuration(n int64) time.Duration {
	return time.Duration(n) * time.Microsecond
}

// durationToMicroseconds returns microseconds from given time.Duration
func durationToMicroseconds(d time.Duration) int64 {
	return int64(d / time.Microsecond)
}

// DateToTime up to 11761191-01-20 00:00:00 +0000 UTC.
func DateToTime(n uint32) time.Time {
	return time.Unix(int64(n)*int64(secondsPerDay), 0).UTC()
}

// DatetimeToTime converts seconds to time.Time
// Up to 2106-02-07 06:28:15 +0000 UTC.
func DatetimeToTime(n uint32) time.Time {
	return time.Unix(int64(n), 0).UTC()
}

// TimestampToTime converts given microseconds to time.Time
// Up to 586524-01-19 08:01:49.000551615 +0000 UTC.
func TimestampToTime(n uint64) time.Time {
	sec := n / 1e6
	nsec := (n - (sec * 1e6)) * 1000
	return time.Unix(int64(sec), int64(nsec)).UTC()
}

func TzDateToTime(s string) (t time.Time, err error) {
	t, err = time.Parse(LayoutTzDate, s)
	if err != nil {
		return
	}
	return t.UTC(), err
}

func TzDatetimeToTime(s string) (t time.Time, err error) {
	t, err = time.Parse(LayoutTzDatetime, s)
	if err != nil {
		return
	}
	return t.UTC(), err
}

func TzTimestampToTime(s string) (t time.Time, err error) {
	t, err = time.Parse(LayoutTzTimestamp, s)
	if err != nil {
		return
	}
	return t.UTC(), err
}
