package xsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/helpers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	internalTable "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	conn interface {
		driver.Conn
		driver.ConnPrepareContext
		driver.ConnBeginTx
		driver.ExecerContext
		driver.QueryerContext
		driver.Pinger
		driver.Validator
		driver.NamedValueChecker

		LastUsage() time.Time
		ID() string
	}
	connWrapper struct {
		conn

		connector      *Connector
		pathNormalizer bind.TablePathPrefix
	}
)

func (c *connWrapper) GetDatabaseName() string {
	return c.connector.Name()
}

func (c *connWrapper) normalizePath(tableName string) string {
	return c.pathNormalizer.NormalizePath(tableName)
}

func (c *connWrapper) tableDescription(ctx context.Context, tableName string) (d options.Description, _ error) {
	d, err := retry.RetryWithResult(ctx, func(ctx context.Context) (options.Description, error) {
		return internalTable.Session(c.ID(), c.connector.balancer, config.New()).DescribeTable(ctx, tableName)
	}, retry.WithIdempotent(true), retry.WithBudget(c.connector.retryBudget), retry.WithTrace(c.connector.traceRetry))
	if err != nil {
		return d, xerrors.WithStackTrace(err)
	}

	return d, nil
}

func (c *connWrapper) GetColumns(ctx context.Context, tableName string) (columns []string, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	for i := range d.Columns {
		columns = append(columns, d.Columns[i].Name)
	}

	return columns, nil
}

func (c *connWrapper) GetColumnType(ctx context.Context, tableName, columnName string) (dataType string, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	for i := range d.Columns {
		if d.Columns[i].Name == columnName {
			return d.Columns[i].Type.Yql(), nil
		}
	}

	return "", xerrors.WithStackTrace(fmt.Errorf("column '%s' not exist in table '%s'", columnName, tableName))
}

func (c *connWrapper) GetPrimaryKeys(ctx context.Context, tableName string) ([]string, error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return d.PrimaryKey, nil
}

func (c *connWrapper) IsPrimaryKey(ctx context.Context, tableName, columnName string) (ok bool, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}

	for _, pkCol := range d.PrimaryKey {
		if pkCol == columnName {
			return true, nil
		}
	}

	return false, nil
}

func (c *connWrapper) Version(_ context.Context) (_ string, _ error) {
	const version = "default"

	return version, nil
}

func isSysDir(databaseName, dirAbsPath string) bool {
	for _, sysDir := range [...]string{
		path.Join(databaseName, ".sys"),
		path.Join(databaseName, ".sys_health"),
	} {
		if strings.HasPrefix(dirAbsPath, sysDir) {
			return true
		}
	}

	return false
}

func (c *connWrapper) getTables(ctx context.Context, absPath string, recursive, excludeSysDirs bool) (
	tables []string, _ error,
) {
	if excludeSysDirs && isSysDir(c.connector.Name(), absPath) {
		return nil, nil
	}

	d, err := c.connector.Scheme().ListDirectory(ctx, absPath)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if !d.IsDirectory() && !d.IsDatabase() {
		return nil, xerrors.WithStackTrace(fmt.Errorf("'%s' is not a folder", absPath))
	}

	for i := range d.Children {
		switch d.Children[i].Type {
		case scheme.EntryTable, scheme.EntryColumnTable:
			tables = append(tables, path.Join(absPath, d.Children[i].Name))
		case scheme.EntryDirectory, scheme.EntryDatabase:
			if recursive {
				childTables, err := c.getTables(ctx, path.Join(absPath, d.Children[i].Name), recursive, excludeSysDirs)
				if err != nil {
					return nil, xerrors.WithStackTrace(err)
				}
				tables = append(tables, childTables...)
			}
		}
	}

	return tables, nil
}

func (c *connWrapper) GetTables(ctx context.Context, folder string, recursive, excludeSysDirs bool) (
	tables []string, _ error,
) {
	absPath := c.normalizePath(folder)

	e, err := c.connector.Scheme().DescribePath(ctx, absPath)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	switch e.Type {
	case scheme.EntryTable, scheme.EntryColumnTable:
		return []string{e.Name}, err
	case scheme.EntryDirectory, scheme.EntryDatabase:
		tables, err = c.getTables(ctx, absPath, recursive, excludeSysDirs)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return xslices.Transform(tables, func(tbl string) string {
			return strings.TrimPrefix(tbl, absPath+"/")
		}), nil
	default:
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("'%s' is not a table or directory (%s)", folder, e.Type.String()),
		)
	}
}

func (c *connWrapper) GetIndexes(ctx context.Context, tableName string) (indexes []string, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return xslices.Transform(d.Indexes, func(idx options.IndexDescription) string {
		return idx.Name
	}), nil
}

func (c *connWrapper) GetIndexColumns(ctx context.Context, tableName, indexName string) (columns []string, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	for i := range d.Indexes {
		if d.Indexes[i].Name == indexName {
			columns = append(columns, d.Indexes[i].IndexColumns...)
		}
	}

	return xslices.Uniq(columns), nil
}

func (c *connWrapper) IsColumnExists(ctx context.Context, tableName, columnName string) (columnExists bool, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}

	for i := range d.Columns {
		if d.Columns[i].Name == columnName {
			return true, nil
		}
	}

	return false, nil
}

func (c *connWrapper) IsTableExists(ctx context.Context, tableName string) (tableExists bool, finalErr error) {
	tableName = c.normalizePath(tableName)
	onDone := trace.DatabaseSQLOnConnIsTableExists(c.connector.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql.(*connWrapper).IsTableExists"),
		tableName,
	)
	defer func() {
		onDone(tableExists, finalErr)
	}()

	tableExists, err := helpers.IsEntryExists(ctx,
		c.connector.parent.Scheme(), tableName,
		scheme.EntryTable, scheme.EntryColumnTable,
	)
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}

	return tableExists, nil
}
