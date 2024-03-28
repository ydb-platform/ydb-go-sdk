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
