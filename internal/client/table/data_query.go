package table

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

type (
	query interface {
		String() string
		ID() string
		YQL() string

		toYDB(a *allocator.Allocator) *Ydb_Table.Query
	}
	textDataQuery     string
	preparedDataQuery struct {
		id    string
		query string
	}
)

func (q textDataQuery) String() string {
	return string(q)
}

func (q textDataQuery) ID() string {
	return ""
}

func (q textDataQuery) YQL() string {
	return string(q)
}

func (q textDataQuery) toYDB(a *allocator.Allocator) *Ydb_Table.Query {
	query := a.TableQuery()
	query.Query = a.TableQueryYqlText(string(q))
	return query
}

func (q preparedDataQuery) String() string {
	return q.query
}

func (q preparedDataQuery) ID() string {
	return q.id
}

func (q preparedDataQuery) YQL() string {
	return q.query
}

func (q preparedDataQuery) toYDB(a *allocator.Allocator) *Ydb_Table.Query {
	query := a.TableQuery()
	query.Query = a.TableQueryID(q.id)
	return query
}

func queryFromText(s string) query {
	return textDataQuery(s)
}

func queryPrepared(id, query string) query {
	return preparedDataQuery{
		id:    id,
		query: query,
	}
}
