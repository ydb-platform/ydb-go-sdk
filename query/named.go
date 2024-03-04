package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
)

func Named(columnName string, destinationValueReference interface{}) (dst scanner.NamedDestination) {
	return scanner.NamedRef(columnName, destinationValueReference)
}
