package options

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	FetchScriptResultsRequest struct {
		Ydb_Query.FetchScriptResultsRequest

		Trace *trace.Query
	}
	FetchScriptOption              func(request *FetchScriptResultsRequest)
	ExecuteScriptOperationMetadata struct {
		ID     string
		Script struct {
			Syntax Syntax
			Query  string
		}
		Mode           ExecMode
		Stats          stats.QueryStats
		ResultSetsMeta []struct {
			Columns []struct {
				Name string
				Type types.Type
			}
		}
	}
	ExecuteScriptOperation struct {
		ID            string
		ConsumedUnits float64
		Metadata      ExecuteScriptOperationMetadata
	}
	FetchScriptResult struct {
		ResultSetIndex int64
		ResultSet      result.Set
		NextToken      string
	}
)

func WithFetchToken(fetchToken string) FetchScriptOption {
	return func(request *FetchScriptResultsRequest) {
		request.FetchToken = fetchToken
	}
}

func WithResultSetIndex(resultSetIndex int64) FetchScriptOption {
	return func(request *FetchScriptResultsRequest) {
		request.ResultSetIndex = resultSetIndex
	}
}

func WithRowsLimit(rowsLimit int64) FetchScriptOption {
	return func(request *FetchScriptResultsRequest) {
		request.RowsLimit = rowsLimit
	}
}
