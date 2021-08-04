package stats

import (
	"bytes"
	"fmt"
	"time"
)

type bucket struct {
	sum float64
	cnt int64
}

func (b *bucket) add(x bucket) {
	b.sum += x.sum
	b.cnt += x.cnt
}

// Series contains logic of accumulating time series data.
type Series struct {
	total   bucket
	current bucket
	buckets []bucket

	duration time.Duration
	touched  time.Time
}

// NewSeries creates time series aggregation with d window size and granularity
// rate n.
//
// The higher the granulatiry, the more accurate and smooth results will be
// provided by series.
func NewSeries(d time.Duration, n int) *Series {
	if d <= 0 {
		panic("ydb: internal: stats: zero or negative duration for series")
	}
	if n <= 0 {
		panic("ydb: internal: stats: zero or negative granularity for series")
	}
	return &Series{
		buckets:  make([]bucket, n),
		duration: d,
	}
}

// Add adds given value x at the time moment of now.
func (s *Series) Add(now time.Time, x float64) {
	s.rotate(now)
	s.current.add(bucket{
		sum: x,
		cnt: 1,
	})
}

// Get returns accumulated data available at the moment of now.
func (s *Series) Get(now time.Time) (sum float64, cnt int64) {
	s.rotate(now)
	return s.total.sum, s.total.cnt
}

// SumPer returns rate of accumulated data available at the moment of now.
func (s *Series) SumPer(now time.Time, period time.Duration) float64 {
	sum, _ := s.Get(now)
	return sum * float64(period) / float64(s.duration)
}

func (s *Series) rotate(now time.Time) {
	if s.touched.IsZero() {
		s.touched = now
		return
	}

	var (
		d     = now.Sub(s.touched) // Elapsed duration.
		n     = len(s.buckets)
		span  = s.duration / time.Duration(n)
		shift = int(d / span)
	)
	if shift == 0 {
		return
	}

	// Slide window strictly on the span size grid.
	s.touched = s.touched.Add(time.Duration(shift) * span)

	if shift > n {
		s.reset()
		return
	}

	current := s.current
	s.current = bucket{}
	s.total = current

	for i := n - 1; i > 0; i-- {
		var prev bucket
		if i-shift >= 0 {
			prev = s.buckets[i-shift]
		}
		s.total.add(prev)
		s.buckets[i] = prev
	}
	s.buckets[shift-1] = current
}

func (s *Series) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%0.f %d ", s.current.sum, s.current.cnt)
	for i, b := range s.buckets {
		if i > 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprintf(&buf, "[%0.f %d]", b.sum, b.cnt)
	}
	fmt.Fprintf(&buf, " = %0.f %d\n", s.total.sum, s.total.cnt)
	return buf.String()
}

func (s *Series) reset() {
	s.current = bucket{}
	for i := range s.buckets {
		s.buckets[i] = bucket{}
	}
}
