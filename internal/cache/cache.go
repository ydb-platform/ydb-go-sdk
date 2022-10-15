package cache

import (
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Cache interface {
	// Enable is used to turn caching on or off
	Enable(on bool)
	// Reset deletes all cached entries
	Reset()
	// Hits reports number of cache hits since last Reset()
	Hits() int
}

type cache struct {
	m map[interface{}]interface{}

	enabled bool
	hits    int
	mx      *sync.RWMutex
}

func newCache() *cache {
	return &cache{
		m:       make(map[interface{}]interface{}),
		enabled: true,
		hits:    0,
		mx:      &sync.RWMutex{},
	}
}

type lazyValue func() (interface{}, error)

func nopLazyValue(value interface{}) lazyValue {
	return func() (interface{}, error) { return value, nil }
}

func (c *cache) loadOrStore(lKey, lValue lazyValue) (interface{}, error) {
	c.mx.Lock()
	defer c.mx.Unlock()

	var key interface{}
	if c.enabled {
		var err error
		key, err = lKey()
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		if val, ok := c.m[key]; ok {
			c.hits++
			return val, nil
		}
	}
	value, err := lValue()
	if c.enabled && err == nil {
		c.m[key] = value
	}
	return value, xerrors.WithStackTrace(err)
}

func (c *cache) Enable(on bool) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.enabled = on
}

func (c *cache) Reset() {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.m = make(map[interface{}]interface{})
	c.hits = 0
}

func (c *cache) Hits() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.hits
}
