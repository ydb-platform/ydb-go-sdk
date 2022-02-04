package table

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

func keepInCache(req *Ydb_Table.ExecuteDataQueryRequest) bool {
	p := req.QueryCachePolicy
	return p != nil && p.KeepInCache
}
