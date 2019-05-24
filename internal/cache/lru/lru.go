package lru

import (
	"container/list"
)

// Cache is not safe for concurrent use.
type Cache struct {
	MaxSize int

	list  list.List
	index map[interface{}]*list.Element
}

func (c *Cache) init() {
	if c.index == nil {
		c.index = make(map[interface{}]*list.Element)
	}
}

func (c *Cache) Add(key, value interface{}) {
	c.init()
	if el, has := c.index[key]; has {
		c.list.MoveToFront(el)
		el.Value.(*entry).value = value
		return
	}
	if c.MaxSize > 0 && c.list.Len() == c.MaxSize {
		c.evictBack()
	}
	c.index[key] = c.list.PushFront(&entry{key, value})
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	el, has := c.index[key]
	if !has {
		return nil, false
	}
	c.list.MoveToFront(el)
	return el.Value.(*entry).value, true
}

func (c *Cache) Remove(key interface{}) (interface{}, bool) {
	el, has := c.index[key]
	if !has {
		return nil, false
	}
	return c.list.Remove(el).(*entry).value, true
}

func (c *Cache) Size() int {
	return c.list.Len()
}

func (c *Cache) Purge() {
	c.list.Init()
	c.index = nil
}

func (c *Cache) evictBack() {
	el := c.list.Back()
	c.list.Remove(el)
	delete(c.index, el.Value.(*entry).key)
}

type entry struct {
	key, value interface{}
}
