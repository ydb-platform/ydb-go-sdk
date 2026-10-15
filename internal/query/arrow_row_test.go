package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestArrowRowScan(t *testing.T) {
	row := &arrowRow{data: &arrowRowData{
		columns: []*Ydb.Column{{Name: "id"}},
		batch:   &arrowTestBatch{rows: [][]types.Value{{types.Int32Value(42)}}},
	}}
	for _, tt := range []struct {
		name    string
		method  string
		context string
		scan    func() error
	}{
		{
			name: "indexed scan", method: "Scan", context: "scan error on column index 0",
			scan: func() error { return row.Scan(new(bool)) },
		},
		{
			name: "named scan", method: "ScanNamed", context: "scan error on column name 'id'",
			scan: func() error { return row.ScanNamed(scanner.NamedRef("id", new(bool))) },
		},
		{
			name: "struct scan", method: "ScanStruct", context: "scan error on struct field name 'id'",
			scan: func() error {
				return row.ScanStruct(&struct {
					ID bool `sql:"id"`
				}{})
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.scan()
			require.Error(t, err)
			require.ErrorIs(t, err, value.ErrCannotCast)
			require.ErrorContains(t, err, tt.context)
			require.ErrorContains(t, err, "cast failed 'Int32(42)' to '*bool' destination")
			require.ErrorContains(t, err, "github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*arrowRow)."+tt.method+"(")
			require.ErrorContains(t, err, "github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestArrowRowScan.func")
		})
	}
}
