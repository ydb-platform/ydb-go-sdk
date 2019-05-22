package ydb

import (
	"testing"
	"time"
)

func TestAbsDuration(t *testing.T) {
	for _, test := range []struct {
		d time.Duration
	}{
		{
			d: time.Minute,
		},
		{
			d: -time.Minute,
		},
	} {
		t.Run(test.d.String(), func(t *testing.T) {
			abs := absDuration(test.d)
			switch {
			case test.d >= 0 && abs+test.d == 2*test.d:
				return
			case abs+test.d == 0:
				return
			}
			t.Errorf("unexpected abs value")
		})
	}
}
