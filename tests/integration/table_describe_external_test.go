//go:build integration
// +build integration

package integration

import (
	"context"
	"path"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestDescribeExternalDataSource(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver()

	dsPath := path.Join(scope.Folder(), "test_external_data_source")

	err := driver.Query().Exec(scope.Ctx,
		"CREATE EXTERNAL DATA SOURCE `"+dsPath+"`"+` WITH (
			SOURCE_TYPE = "ObjectStorage",
			LOCATION = "localhost:12345",
			AUTH_METHOD = "NONE"
		)`,
	)
	if ydb.IsTransportError(err, codes.Unimplemented) {
		t.Skip("external data sources are not supported in this YDB version")
	}
	scope.Require.NoError(err)

	var desc options.ExternalDataSourceDescription
	err = driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
		var descErr error
		desc, descErr = s.DescribeExternalDataSource(ctx, "/"+dsPath)

		return descErr
	}, table.WithIdempotent())
	scope.Require.NoError(err)

	scope.Require.Equal("test_external_data_source", desc.Name)
	scope.Require.Equal("ObjectStorage", desc.SourceType)
	scope.Require.Equal("localhost:12345", desc.Location)
	scope.Require.Contains(desc.Properties, "AUTH_METHOD")
	scope.Require.Equal("NONE", desc.Properties["AUTH_METHOD"])
}

func TestDescribeExternalTable(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver()

	folder := scope.Folder()
	dsPath := path.Join(folder, "test_ext_ds_for_table")
	escapedDsPath := "`" + dsPath + "`"

	err := driver.Query().Exec(scope.Ctx,
		"CREATE EXTERNAL DATA SOURCE "+escapedDsPath+` WITH (
			SOURCE_TYPE = "ObjectStorage",
			LOCATION = "localhost:12345",
			AUTH_METHOD = "NONE"
		)`,
	)
	if ydb.IsTransportError(err, codes.Unimplemented) {
		t.Skip("external data sources are not supported in this YDB version")
	}
	scope.Require.NoError(err)

	tablePath := path.Join(folder, "test_external_table")
	escapedTablePath := "`" + tablePath + "`"
	escapedDsPathForTable := `"` + dsPath + `"`

	err = driver.Query().Exec(scope.Ctx,
		"CREATE EXTERNAL TABLE "+escapedTablePath+` (
			key Utf8 NOT NULL,
			value Utf8 NOT NULL
		) WITH (
			DATA_SOURCE = `+escapedDsPathForTable+`,
			LOCATION = "/test/",
			FORMAT = "csv_with_names"
		)`,
	)
	scope.Require.NoError(err)

	var desc options.ExternalTableDescription
	err = driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
		var descErr error
		desc, descErr = s.DescribeExternalTable(ctx, tablePath)

		return descErr
	}, table.WithIdempotent())
	scope.Require.NoError(err)

	scope.Require.Equal("test_external_table", desc.Name)
	scope.Require.Equal("ObjectStorage", desc.SourceType)
	scope.Require.Equal(dsPath, desc.DataSourcePath)
	scope.Require.Equal("/test/", desc.Location)
	scope.Require.Len(desc.Columns, 2)
	scope.Require.Equal("key", desc.Columns[0].Name)
	scope.Require.Equal("value", desc.Columns[1].Name)
}
