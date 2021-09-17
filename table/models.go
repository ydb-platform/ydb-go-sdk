package table

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func NewTableColumn(name string, typ types.Type, family string) options.Column {
	return options.Column{
		Name:   name,
		Type:   typ,
		Family: family,
	}
}
