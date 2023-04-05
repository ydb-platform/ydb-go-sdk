package bind

import (
	"path"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

type TablePathPrefix string

func (tablePathPrefix TablePathPrefix) NormalizePath(folderOrTable string) string {
	switch ch := folderOrTable[0]; ch {
	case '/':
		return folderOrTable
	case '.':
		return path.Join(string(tablePathPrefix), strings.TrimLeft(folderOrTable, "."))
	default:
		return path.Join(string(tablePathPrefix), folderOrTable)
	}
}

func (tablePathPrefix TablePathPrefix) RewriteQuery(query string, args ...interface{}) (
	yql string, newArgs []interface{}, err error,
) {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)

	buffer.WriteString("-- bind TablePathPrefix\n")
	buffer.WriteString("PRAGMA TablePathPrefix(\"")
	buffer.WriteString(string(tablePathPrefix))
	buffer.WriteString("\");\n\n")
	buffer.WriteString(query)

	return buffer.String(), args, nil
}
