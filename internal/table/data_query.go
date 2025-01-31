package table

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

type (
	Query interface {
		String() string
		ID() string
		YQL() string

		toYDB(a *allocator.Allocator) *Ydb_Table.Query
	}
	textQuery     string
	preparedQuery struct {
		id  string
		sql string
	}
)

func (q textQuery) String() string {
	return string(q)
}

func (q textQuery) ID() string {
	return ""
}

func (q textQuery) YQL() string {
	return string(q)
}

func (q textQuery) toYDB(a *allocator.Allocator) *Ydb_Table.Query {
	query := a.TableQuery()
	query.Query = a.TableQueryYqlText(string(q))

	return query
}

func (q preparedQuery) String() string {
	return q.sql
}

func (q preparedQuery) ID() string {
	return q.id
}

func (q preparedQuery) YQL() string {
	return q.sql
}

func (q preparedQuery) toYDB(a *allocator.Allocator) *Ydb_Table.Query {
	query := a.TableQuery()
	query.Query = a.TableQueryID(q.id)

	return query
}

func queryFromText(s string) Query {
	return textQuery(s)
}

func queryPrepared(id, sql string) Query {
	return preparedQuery{
		id:  id,
		sql: sql,
	}
}
