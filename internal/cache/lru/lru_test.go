package lru

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestCache(t *testing.T) {
	c := Cache{
		MaxSize: 100,
	}
	rands := randStr()
	pairs := make([]entry, c.MaxSize*100)
	for i := range pairs {
		var (
			key   = rands()
			value = rands()
		)
		c.Add(key, value)
		pairs[i] = entry{key, value}
	}
	bound := len(pairs) - c.MaxSize
	for i, p := range pairs {
		x, ok := c.Get(p.key)
		switch {
		case i >= bound && !ok:
			t.Errorf("unexpected miss for #%dth entry", i)
		case i >= bound && x != p.value:
			t.Errorf("unexpected #%dth value: %v; want %v", i, x, p.value)
		case i < bound && ok:
			t.Errorf("unexpected hit for #%dth entry", i)
		}
	}
}

func randStr() func() string {
	used := make(map[string]struct{})
	return func() string {
		for {
			s := strconv.FormatUint(rand.Uint64(), 32)
			if _, seen := used[s]; !seen {
				return s
			}
		}
	}
}
