package ydbx

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"sync"
)

type tableWrapper struct {
	driver   ydb.Driver
	once     sync.Once
	instance *table.SessionPool
}

func newTableWrapper(driver ydb.Driver) *tableWrapper {
	return &tableWrapper{
		driver: driver,
	}
}

func newSessionPool(driver ydb.Driver) *table.SessionPool {
	return &table.SessionPool{
		Builder: &table.Client{
			Driver: driver,
		},
	}
}

func (t *tableWrapper) Pool() *table.SessionPool {
	t.once.Do(func() {
		t.instance = newSessionPool(t.driver)
	})
	return t.instance
}
