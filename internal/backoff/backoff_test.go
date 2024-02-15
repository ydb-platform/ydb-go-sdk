package backoff

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDelays(t *testing.T) {
	duration := func(s string) (d time.Duration) {
		d, err := time.ParseDuration(s)
		if err != nil {
			panic(err)
		}

		return d
	}
	b := New(
		WithSlotDuration(duration("500ms")),
		WithCeiling(6),
		WithJitterLimit(1),
		WithSeed(0),
	)
	for i, d := range map[int]time.Duration{
		0: duration("500ms"),
		1: duration("1s"),
		2: duration("2s"),
		3: duration("4s"),
		4: duration("8s"),
		5: duration("16s"),
		6: duration("32s"),
		7: duration("32s"),
		8: duration("32s"),
	} {
		t.Run(fmt.Sprintf("%v -> %v", i, d), func(t *testing.T) {
			require.Equal(t, d, b.Delay(i))
		})
	}
}

func TestLogBackoff(t *testing.T) {
	type exp struct {
		eq  time.Duration
		gte time.Duration
		lte time.Duration
	}
	for _, tt := range []struct {
		backoff Backoff
		exp     []exp
		seeds   int64
	}{
		{
			backoff: New(
				WithSlotDuration(time.Second),
				WithCeiling(3),
				WithJitterLimit(0),
				WithSeed(0),
			),
			exp: []exp{
				{gte: 0, lte: time.Second},
				{gte: 0, lte: 2 * time.Second},
				{gte: 0, lte: 4 * time.Second},
				{gte: 0, lte: 8 * time.Second},
				{gte: 0, lte: 8 * time.Second},
				{gte: 0, lte: 8 * time.Second},
				{gte: 0, lte: 8 * time.Second},
			},
			seeds: 1000,
		},
		{
			backoff: New(
				WithSlotDuration(time.Second),
				WithCeiling(3),
				WithJitterLimit(0.5),
				WithSeed(0),
			),
			exp: []exp{
				{gte: 500 * time.Millisecond, lte: time.Second},
				{gte: 1 * time.Second, lte: 2 * time.Second},
				{gte: 2 * time.Second, lte: 4 * time.Second},
				{gte: 4 * time.Second, lte: 8 * time.Second},
				{gte: 4 * time.Second, lte: 8 * time.Second},
				{gte: 4 * time.Second, lte: 8 * time.Second},
				{gte: 4 * time.Second, lte: 8 * time.Second},
			},
			seeds: 1000,
		},
		{
			backoff: New(
				WithSlotDuration(time.Second),
				WithCeiling(3),
				WithJitterLimit(1),
				WithSeed(0),
			),
			exp: []exp{
				{eq: time.Second},
				{eq: 2 * time.Second},
				{eq: 4 * time.Second},
				{eq: 8 * time.Second},
				{eq: 8 * time.Second},
				{eq: 8 * time.Second},
				{eq: 8 * time.Second},
			},
		},
		{
			backoff: New(
				WithSlotDuration(time.Second),
				WithCeiling(6),
				WithJitterLimit(1),
				WithSeed(0),
			),
			exp: []exp{
				{eq: time.Second},
				{eq: 2 * time.Second},
				{eq: 4 * time.Second},
				{eq: 8 * time.Second},
				{eq: 16 * time.Second},
				{eq: 32 * time.Second},
				{eq: 64 * time.Second},
				{eq: 64 * time.Second},
				{eq: 64 * time.Second},
				{eq: 64 * time.Second},
				{eq: 64 * time.Second},
				{eq: 64 * time.Second},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			if tt.seeds == 0 {
				tt.seeds = 1
			}
			for seed := int64(0); seed < tt.seeds; seed++ {
				for n, exp := range tt.exp {
					act := tt.backoff.Delay(n)
					if eq := exp.eq; eq != 0 {
						if eq != act {
							t.Fatalf(
								"unexpected Backoff delay: %s; want %s",
								act, eq,
							)
						}

						continue
					}
					if gte := exp.gte; act <= gte {
						t.Errorf(
							"unexpected Backoff delay: %s; want >= %s",
							act, gte,
						)
					}
					if lte := exp.lte; act >= lte {
						t.Errorf(
							"unexpected Backoff delay: %s; want <= %s",
							act, lte,
						)
					}
				}
			}
		})
	}
}

func TestFastSlowDelaysWithoutJitter(t *testing.T) {
	for _, tt := range []struct {
		name    string
		backoff Backoff
		exp     []time.Duration
	}{
		{
			name: "FastBackoff",
			backoff: func() (backoff logBackoff) {
				backoff = Fast
				backoff.jitterLimit = 1

				return backoff
			}(),
			exp: []time.Duration{
				5 * time.Millisecond,
				10 * time.Millisecond,
				20 * time.Millisecond,
				40 * time.Millisecond,
				80 * time.Millisecond,
				160 * time.Millisecond,
				320 * time.Millisecond,
				320 * time.Millisecond,
				320 * time.Millisecond,
				320 * time.Millisecond,
				320 * time.Millisecond,
			},
		},
		{
			name: "SlowBackoff",
			backoff: func() (backoff logBackoff) {
				backoff = Slow
				backoff.jitterLimit = 1

				return backoff
			}(),
			exp: []time.Duration{
				time.Second,
				2 * time.Second,
				4 * time.Second,
				8 * time.Second,
				16 * time.Second,
				32 * time.Second,
				64 * time.Second,
				64 * time.Second,
				64 * time.Second,
				64 * time.Second,
				64 * time.Second,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for n, exp := range tt.exp {
				t.Run("delay#"+strconv.Itoa(n), func(t *testing.T) {
					act := tt.backoff.Delay(n)
					require.Equal(t, exp, act)
				})
			}
		})
	}
}
