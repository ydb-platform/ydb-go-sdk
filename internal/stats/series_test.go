package stats

import (
	"testing"
	"time"
)

func TestSeries(t *testing.T) {
	type result struct {
		sum float64
		cnt int64
	}
	for _, test := range []struct {
		name string

		duration time.Duration
		buckets  int

		step time.Duration
		exp  map[time.Duration]result
	}{
		{
			duration: time.Minute,
			buckets:  4,

			step: time.Second,
			exp: map[time.Duration]result{
				15 * time.Second: {
					sum: 15,
					cnt: 15,
				},
				30 * time.Second: {
					sum: 30,
					cnt: 30,
				},
				45 * time.Second: {
					sum: 45,
					cnt: 45,
				},
				60 * time.Second: {
					sum: 60,
					cnt: 60,
				},
				75 * time.Second: {
					sum: 60,
					cnt: 60,
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := NewSeries(test.duration, test.buckets)

			var max time.Duration = -1
			for k := range test.exp {
				if k > max {
					max = k
				}
			}
			for i := time.Duration(0); i <= max; i += test.step {
				now := time.Unix(0, 0).Add(i)
				s.Add(now, 1)
				if exp, ok := test.exp[i]; ok {
					t.Logf("%s", s)
					sum, cnt := s.Get(now)
					act := result{
						sum: sum,
						cnt: cnt,
					}
					if act != exp {
						t.Errorf(
							"unexpected value after %s: %+v; want %+v",
							i, act, exp,
						)
					}
				}
			}
		})
	}
}

func TestSeriesRareEvents(t *testing.T) {
	s := NewSeries(4*time.Second, 1)
	for i, a := range []struct {
		time   time.Time
		x      float64
		expSum float64
		expCnt int64
	}{
		{
			time:   time.Unix(0, 0),
			x:      1,
			expSum: 0,
			expCnt: 0,
		},
		{
			time:   time.Unix(5, 0),
			x:      2,
			expSum: 1,
			expCnt: 1,
		},
		{
			time:   time.Unix(8, 0),
			x:      3,
			expSum: 2,
			expCnt: 1,
		},
	} {
		s.Add(a.time, a.x)
		t.Logf("%s", s)
		sum, cnt := s.Get(a.time)
		if act, exp := sum, a.expSum; act != exp {
			t.Errorf(
				"unexpected sum after #%d action: %v; want %v",
				i, act, exp,
			)
		}
		if act, exp := cnt, a.expCnt; act != exp {
			t.Errorf(
				"unexpected count after #%d action: %v; want %v",
				i, act, exp,
			)
		}
	}
}
