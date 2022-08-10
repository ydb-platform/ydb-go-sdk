package xrand

import (
	"math"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func Test_r_Int64(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		for _, tt := range []struct {
			n       int
			epsilon float64
		}{
			{10000000, 0.004},
			{1000000, 0.012},
			{100000, 0.035},
			{10000, 0.14},
			{1000, 0.44},
			{100, 1.2},
			{10, 5.0},
		} {
			var (
				bucketsLen = 10
				exp        = tt.n / bucketsLen
				min        = exp - int(float64(exp)*tt.epsilon)
				max        = exp + int(float64(exp)*tt.epsilon)
				buckets    = make([]int, bucketsLen)
				r          = New()
			)
			for i := 0; i < tt.n; i++ {
				buckets[r.Int64(int64(bucketsLen))]++
			}
			for i, v := range buckets {
				if v < min || v > max {
					t.Errorf("%+v: buckets[%d] = %d (delta = %f)", tt, i, v, math.Abs(float64(exp-v))/float64(exp))
				}
			}
		}
	}, xtest.StopAfter(42*time.Second))
}
