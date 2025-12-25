package value

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTzSomeToTime(t *testing.T) {
	for _, tt := range []struct {
		name      string
		src       string
		exp       time.Time
		converter func(string) (time.Time, error)
	}{
		{
			"TzDateToTime",
			"2020-05-29,Europe/Berlin",
			time.Date(2020, time.May, 29, 0, 0, 0, 0,
				func() *time.Location {
					l, _ := time.LoadLocation("Europe/Berlin")

					return l
				}(),
			),
			TzDateToTime,
		},
		{
			"TzDatetimeToTime",
			"2020-05-29T11:22:54,Europe/Berlin",
			time.Date(2020, time.May, 29, 11, 22, 54, 0,
				func() *time.Location {
					l, _ := time.LoadLocation("Europe/Berlin")

					return l
				}(),
			),
			TzDatetimeToTime,
		},
		{
			"TzTimestampToTime",
			"2020-05-29T11:22:54.123456,Europe/Berlin",
			time.Date(2020, time.May, 29, 11, 22, 54, 123456000,
				func() *time.Location {
					l, _ := time.LoadLocation("Europe/Berlin")

					return l
				}(),
			),
			TzTimestampToTime,
		},
		{
			"TzTimestampToTime",
			"2020-05-29T11:22:54,Europe/Berlin",
			time.Date(2020, time.May, 29, 11, 22, 54, 0,
				func() *time.Location {
					l, _ := time.LoadLocation("Europe/Berlin")

					return l
				}(),
			),
			TzTimestampToTime,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.converter(tt.src)
			require.NoError(t, err)
			require.Equal(t, tt.exp, v, v.Format(LayoutDate))
		})
	}
}

func TestInterval64ToDuration(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    int64
		expected time.Duration
	}{
		{
			name:     "zero",
			input:    0,
			expected: 0,
		},
		{
			name:     "one microsecond",
			input:    1,
			expected: time.Microsecond,
		},
		{
			name:     "one second",
			input:    int64(time.Second / time.Microsecond),
			expected: time.Second,
		},
		{
			name:     "one hour",
			input:    int64(time.Hour / time.Microsecond),
			expected: time.Hour,
		},
		{
			name:     "negative value",
			input:    -1000,
			expected: -time.Millisecond,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := Interval64ToDuration(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDurationToNanoseconds(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    time.Duration
		expected int64
	}{
		{
			name:     "zero",
			input:    0,
			expected: 0,
		},
		{
			name:     "one nanosecond",
			input:    time.Nanosecond,
			expected: 1,
		},
		{
			name:     "one microsecond",
			input:    time.Microsecond,
			expected: 1000,
		},
		{
			name:     "one millisecond",
			input:    time.Millisecond,
			expected: 1000000,
		},
		{
			name:     "one second",
			input:    time.Second,
			expected: 1000000000,
		},
		{
			name:     "one hour",
			input:    time.Hour,
			expected: 3600000000000,
		},
		{
			name:     "negative duration",
			input:    -time.Millisecond,
			expected: -1000000,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := durationToNanoseconds(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
