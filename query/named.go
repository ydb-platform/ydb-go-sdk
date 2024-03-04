package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
)

func Named(columnName string, destinationValueReference interface{}) scanner.NamedDestination {
	return scanner.NamedRef(columnName, destinationValueReference)
}
