package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Column = (*column)(nil)

type column struct {
	n string
	t query.Type
}

func newColumn(c *Ydb.Column) *column {
	return &column{
		n: c.GetName(),
		t: types.TypeFromYDB(c.GetType()),
	}
}

func newColumns(cc []*Ydb.Column) (columns []query.Column) {
	columns = make([]query.Column, len(cc))
	for i := range cc {
		columns[i] = newColumn(cc[i])
	}
	return columns
}

func (c *column) Name() string {
	return c.n
}

func (c *column) Type() query.Type {
	return c.t
}
