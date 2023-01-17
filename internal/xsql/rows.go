package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	_ driver.Rows                           = &rows{}
	_ driver.RowsNextResultSet              = &rows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &rows{}
	_ driver.Rows                           = &single{}

	_ types.Scanner = &valuer{}
)

type cacheMode int32

const (
	columnsCache cacheMode = cacheMode(iota)
	typesCache
)

type cache struct {
	data map[cacheMode][]string
	done map[cacheMode]bool
}

func (c *cache) get(key cacheMode) []string {
	return c.data[key]
}

func (c *cache) set(key cacheMode, val []string) {
	c.data[key] = val
	c.done[key] = true
}

func (c *cache) cached(key cacheMode) bool {
	if _, has := c.done[key]; !has {
		return false
	}
	return c.done[key]
}

func (c *cache) reset(key cacheMode) {
	c.done[key] = false
}

func (c *cache) resetAll() {
	for key := range c.done {
		c.reset(key)
	}
}

func newCache() *cache {
	return &cache{
		data: make(map[cacheMode][]string),
		done: make(map[cacheMode]bool),
	}
}

type rows struct {
	*cache
	conn   *conn
	result result.BaseResult

	// nextSet once need for get first result set as default.
	// Iterate over many result sets must be with rows.NextResultSet()
	nextSet sync.Once
}

func (r *rows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (r *rows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

func (r *rows) Columns() []string {
	r.nextSet.Do(func() {
		r.result.NextResultSet(context.Background())
		if r.cache == nil {
			r.cache = newCache()
		}
	})

	if r.cache.cached(columnsCache) {
		cs := make([]string, 0)
		cs = append(cs, r.cache.get(columnsCache)...)
		return cs
	}

	var i int
	cs := make([]string, r.result.CurrentResultSet().ColumnCount())
	r.result.CurrentResultSet().Columns(func(m options.Column) {
		cs[i] = m.Name
		i++
	})

	r.cache.set(columnsCache, cs)
	return cs
}

// https://cs.opensource.google/go/go/+/refs/tags/go1.19.4:src/database/sql/sql.go;l=3101
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	r.nextSet.Do(func() {
		r.result.NextResultSet(context.Background())
		if r.cache == nil {
			r.cache = newCache()
		}
	})

	if r.cache.cached(typesCache) {
		return r.cache.get(typesCache)[index]
	}

	var i int
	yqlTypes := make([]string, r.result.CurrentResultSet().ColumnCount())
	r.result.CurrentResultSet().Columns(func(m options.Column) {
		yqlTypes[i] = m.Type.Yql()
		i++
	})

	r.cache.set(typesCache, yqlTypes)
	return yqlTypes[index]
}

func (r *rows) NextResultSet() error {
	r.nextSet.Do(func() {
		if r.cache == nil {
			r.cache = newCache()
		}
	})
	r.cache.resetAll()
	return r.result.NextResultSetErr(context.Background())
}

func (r *rows) HasNextResultSet() bool {
	return r.result.HasNextResultSet()
}

func (r *rows) Next(dst []driver.Value) (err error) {
	r.nextSet.Do(func() {
		err = r.result.NextResultSetErr(context.Background())
		if r.cache == nil {
			r.cache = newCache()
		}
	})
	if err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	if err = r.result.Err(); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	if !r.result.NextRow() {
		return io.EOF
	}
	values := make([]indexed.RequiredOrOptional, len(dst))
	for i := range dst {
		values[i] = &valuer{}
	}
	if err = r.result.Scan(values...); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	for i := range values {
		dst[i] = values[i].(*valuer).Value()
	}
	if err = r.result.Err(); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	return nil
}

func (r *rows) Close() error {
	return r.result.Close()
}

type single struct {
	values []sql.NamedArg
}

func (r *single) Columns() (columns []string) {
	for _, v := range r.values {
		columns = append(columns, v.Name)
	}
	return columns
}

func (r *single) Close() error {
	return nil
}

func (r *single) Next(dst []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	for i := range r.values {
		dst[i] = r.values[i].Value
	}
	r.values = nil
	return nil
}
