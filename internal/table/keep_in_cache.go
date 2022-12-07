package table

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

func keepInCache(p *Ydb_Table.QueryCachePolicy) bool {
	return p != nil && p.KeepInCache
}
