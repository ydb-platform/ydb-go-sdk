package ydb

import (
	"math/rand"
	"testing"
	"time"
)

func TestLogBackoff(t *testing.T) {
	type exp struct {
		eq  time.Duration
		gte time.Duration
		lte time.Duration
	}
	for _, test := range []struct {
		name    string
		backoff LogBackoff
		exp     []exp
		seeds   int64
	}{
		{
			backoff: LogBackoff{
				SlotDuration: time.Second,
				Ceiling:      3,
				JitterLimit:  0,
			},
			exp: []exp{
				{gte: 0, lte: time.Second},     // 1 << min(0, 3)
				{gte: 0, lte: 2 * time.Second}, // 1 << min(1, 3)
				{gte: 0, lte: 4 * time.Second}, // 1 << min(2, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(3, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(4, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(5, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(6, 3)
			},
			seeds: 1000,
		},
		{
			backoff: LogBackoff{
				SlotDuration: time.Second,
				Ceiling:      3,
				JitterLimit:  0.5,
			},
			exp: []exp{
				{gte: 500 * time.Millisecond, lte: time.Second}, // 1 << min(0, 3)
				{gte: 1 * time.Second, lte: 2 * time.Second},    // 1 << min(1, 3)
				{gte: 2 * time.Second, lte: 4 * time.Second},    // 1 << min(2, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(3, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(4, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(5, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(6, 3)
			},
			seeds: 1000,
		},
		{
			backoff: LogBackoff{
				SlotDuration: time.Second,
				Ceiling:      3,
				JitterLimit:  1,
			},
			exp: []exp{
				{eq: time.Second},     // 1 << min(0, 3)
				{eq: 2 * time.Second}, // 1 << min(1, 3)
				{eq: 4 * time.Second}, // 1 << min(2, 3)
				{eq: 8 * time.Second}, // 1 << min(3, 3)
				{eq: 8 * time.Second}, // 1 << min(4, 3)
				{eq: 8 * time.Second}, // 1 << min(5, 3)
				{eq: 8 * time.Second}, // 1 << min(6, 3)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.seeds == 0 {
				test.seeds = 1
			}
			for seed := int64(0); seed < test.seeds; seed++ {
				// Fix random to reproduce the tests.
				rand.Seed(seed)

				for n, exp := range test.exp {
					act := test.backoff.delay(n)
					if exp := exp.eq; exp != 0 {
						if exp != act {
							t.Fatalf(
								"unexpected backoff delay: %s; want %s",
								act, exp,
							)
						}
						continue
					}
					if gte := exp.gte; act < gte {
						t.Errorf(
							"unexpected backoff delay: %s; want >= %s",
							act, gte,
						)
					}
					if lte := exp.lte; act > lte {
						t.Errorf(
							"unexpected backoff delay: %s; want <= %s",
							act, lte,
						)
					}
				}
			}
		})
	}
}
