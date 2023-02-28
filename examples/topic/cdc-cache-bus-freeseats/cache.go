package main

import (
	"sync"
	"time"
)

type Cache struct {
	timeout time.Duration

	m          sync.Mutex
	setCounter int
	values     map[string]CacheItem
}

func NewCache(timeout time.Duration) *Cache {
	return &Cache{
		timeout: timeout,
		values:  make(map[string]CacheItem),
	}
}

func (c *Cache) Get(key string) (int64, bool) {
	if c.timeout == 0 {
		return 0, false
	}

	c.m.Lock()
	defer c.m.Unlock()

	item, ok := c.values[key]
	if !ok {
		return 0, false
	}

	if time.Now().After(item.ExpiresAt) {
		delete(c.values, key)
		return 0, false
	}

	return item.Value, true
}

func (c *Cache) Set(key string, value int64) {
	if c.timeout == 0 {
		return
	}

	c.set(time.Now(), key, value)
}

func (c *Cache) Delete(key string) {
	if c.timeout == 0 {
		return
	}

	c.m.Lock()
	defer c.m.Unlock()

	delete(c.values, key)
}

func (c *Cache) set(now time.Time, key string, value int64) {
	c.m.Lock()
	defer c.m.Unlock()

	c.setCounter++
	if c.setCounter%1000 == 0 {
		c.cleanupNeedLock(now)
	}

	c.values[key] = CacheItem{
		Value:     value,
		ExpiresAt: now.Add(c.timeout),
	}
}

func (c *Cache) cleanupNeedLock(now time.Time) {
	for key, item := range c.values {
		if now.After(item.ExpiresAt) {
			delete(c.values, key)
		}
	}
}

type CacheItem struct {
	ExpiresAt time.Time
	Value     int64
}
