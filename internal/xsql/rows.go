package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/helpers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	_ driver.Rows                           = &rows{}
	_ driver.RowsNextResultSet              = &rows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &rows{}
	_ driver.Rows                           = &single{}

	_ types.Scanner = &valuer{}
)

type rows struct {
	conn    *conn
	result  result.BaseResult
	nextSet sync.Once
}

func (r *rows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (r *rows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

func (r *rows) Columns() []string {
	r.nextSet.Do(func() {
		r.result.NextResultSet(context.Background())
	})
	var i int
	cs := make([]string, r.result.CurrentResultSet().ColumnCount())
	r.result.CurrentResultSet().Columns(func(m options.Column) {
		cs[i] = m.Name
		i++
	})
	return cs
}

// NOTE: Need to optimize somehow? This might take O(|r.Columns()|^2)
// each time (*Rows).ColumnTypes be called
// https://cs.opensource.google/go/go/+/refs/tags/go1.19.4:src/database/sql/sql.go;l=3101
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	r.nextSet.Do(func() {
		r.result.NextResultSet(context.Background())
	})

	var i int
	yqlTypes := make([]string, r.result.CurrentResultSet().ColumnCount())
	r.result.CurrentResultSet().Columns(func(m options.Column) {
		yqlTypes[i] = m.Type.Yql()
		i++
	})
	return yqlTypes[index]
}

func (r *rows) NextResultSet() error {
	r.nextSet.Do(func() {})
	return r.result.NextResultSetErr(context.Background())
}

func (r *rows) HasNextResultSet() bool {
	return r.result.HasNextResultSet()
}

func (r *rows) Next(dst []driver.Value) (err error) {
	r.nextSet.Do(func() {
		err = r.result.NextResultSetErr(context.Background())
	})
	if err != nil {
		return r.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	if err = r.result.Err(); err != nil {
		return r.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	if !r.result.NextRow() {
		return io.EOF
	}
	values := make([]indexed.RequiredOrOptional, len(dst))
	for i := range dst {
		values[i] = &valuer{}
	}
	if err = r.result.Scan(values...); err != nil {
		return r.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	for i := range values {
		dst[i] = values[i].(*valuer).Value()
	}
	if err = r.result.Err(); err != nil {
		return r.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	return nil
}

func (r *rows) Close() error {
	return r.result.Close()
}

type single struct {
	values []sql.NamedArg
}

func (r *single) Columns() (columns []string) {
	for _, v := range r.values {
		columns = append(columns, v.Name)
	}
	return columns
}

func (r *single) Close() error {
	return nil
}

func (r *single) Next(dst []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	for i := range r.values {
		dst[i] = r.values[i].Value
	}
	r.values = nil
	return nil
}

type multiRows struct {
	rows
	columns     []string
	values      [][]sql.NamedArg
	currentRow  int
	initColumns bool
}

var _ driver.Rows = &multiRows{}

func (mr *multiRows) Columns() (columns []string) {
	if mr.currentRow >= len(mr.values) {
		return
	}
	if !mr.initColumns {
		if mr.columns == nil {
			mr.columns = make([]string, len(mr.values[mr.currentRow]))
		}
		for i := range mr.values[mr.currentRow] {
			mr.columns[i] = mr.values[mr.currentRow][i].Name
		}
		mr.initColumns = true
	}
	columns = append(columns, mr.columns...)
	return columns
}

func (mr *multiRows) Close() error {
	return nil
}

func (mr *multiRows) Next(dst []driver.Value) error {
	if mr.currentRow >= len(mr.values) {
		return io.EOF
	}
	if !mr.initColumns {
		if mr.columns == nil {
			mr.columns = make([]string, len(mr.values[mr.currentRow]))
		}
		for i := range mr.values[mr.currentRow] {
			mr.columns[i] = mr.values[mr.currentRow][i].Name
		}
		mr.initColumns = true
	}

	for i := range mr.values[mr.currentRow] {
		dst[i] = mr.values[mr.currentRow][i].Value
	}

	mr.currentRow++
	return nil
}

func (mr *multiRows) isTableExist(ctx context.Context, _ string, args []driver.NamedValue) (err error) {
	margs := make(map[string]driver.Value)
	for _, arg := range args {
		margs[arg.Name] = arg.Value
	}
	if _, has := margs["TableName"]; !has {
		return fmt.Errorf("table name not found")
	}

	var (
		tableName = margs["TableName"].(string)
		cn        = mr.conn.connector.Connection()
	)

	exist, err := helpers.IsTableExists(ctx, cn.Scheme(), tableName)
	if err != nil {
		return err
	}

	mr.values = make([][]sql.NamedArg, 0)
	if exist {
		mr.values = append(mr.values, []sql.NamedArg{sql.Named("TableName", tableName)})
	}
	return
}

func (mr *multiRows) isColumnExist(ctx context.Context, _ string, args []driver.NamedValue) (err error) {
	margs := make(map[string]driver.Value)
	for _, arg := range args {
		margs[arg.Name] = arg.Value
	}

	if _, has := margs["TableName"]; !has {
		return fmt.Errorf("table name not found")
	}
	if _, has := margs["ColumnName"]; !has {
		return fmt.Errorf("column name not found")
	}

	var (
		tableName   = margs["TableName"].(string)
		columnName  = margs["ColumnName"].(string)
		columnExist = false
		cn          = mr.conn.connector.Connection()
	)

	tableExist, err := helpers.IsTableExists(ctx, cn.Scheme(), tableName)
	if err != nil {
		return err
	}
	if !tableExist {
		return fmt.Errorf("table '%s' not exist", tableName)
	}

	session := mr.conn.session
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		desc, err := session.DescribeTable(ctx, tableName)
		if err != nil {
			return
		}
		for _, col := range desc.Columns {
			if col.Name == columnName {
				columnExist = true
				break
			}
		}
		return
	}, retry.WithIdempotent(true))
	if err != nil {
		return err
	}

	mr.values = make([][]sql.NamedArg, 0)
	if columnExist {
		mr.values = append(mr.values, []sql.NamedArg{sql.Named("ColumnName", columnName)})
	}
	return err
}

func (mr *multiRows) getColumns(ctx context.Context, _ string, args []driver.NamedValue) (err error) {
	margs := make(map[string]driver.Value)
	for _, arg := range args {
		margs[arg.Name] = arg.Value
	}
	if _, has := margs["TableName"]; !has {
		return fmt.Errorf("table name not found")
	}

	var (
		tableName = margs["TableName"].(string)
		cn        = mr.conn.connector.Connection()
	)

	mr.values = make([][]sql.NamedArg, 0)

	tableExist, err := helpers.IsTableExists(ctx, cn.Scheme(), tableName)
	if err != nil {
		return err
	}
	if !tableExist {
		return fmt.Errorf("table '%s' not exist", tableName)
	}

	session := mr.conn.session
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		desc, err := session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}

		isPk := make(map[string]bool)
		for _, pk := range desc.PrimaryKey {
			isPk[pk] = true
		}

		for _, col := range desc.Columns {
			mr.values = append(mr.values, []sql.NamedArg{
				sql.Named("ColumnName", col.Name),
				sql.Named("TableName", tableName),
				sql.Named("DataType", col.Type.Yql()),
				sql.Named("IsPrimaryKey", isPk[col.Name]),
			})
		}
		return
	}, retry.WithIdempotent(true))
	if err != nil {
		return err
	}
	return err
}

func (mr *multiRows) getTables(ctx context.Context, _ string, args []driver.NamedValue) (err error) {
	margs := make(map[string]driver.Value)
	for _, arg := range args {
		margs[arg.Name] = arg.Value
	}
	if _, has := margs["DatabaseName"]; !has {
		return fmt.Errorf("database name not found")
	}

	var (
		databaseName = margs["DatabaseName"].(string)
		cn           = mr.conn.connector.Connection()
		schemeClient = cn.Scheme()
	)

	ignoreDirs := map[string]bool{
		".sys":        true,
		".sys_health": true,
	}

	mr.values = make([][]sql.NamedArg, 0)

	canEnter := func(dir string) bool {
		if _, ignore := ignoreDirs[dir]; ignore {
			return false
		}
		return true
	}

	queue := make([]string, 0)
	queue = append(queue, databaseName)

	for st := 0; st < len(queue); st++ {
		curDir := queue[st]

		var e scheme.Entry
		err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			e, err = schemeClient.DescribePath(ctx, curDir)
			return
		}, retry.WithIdempotent(true))

		if err != nil {
			return err
		}

		if e.IsTable() {
			mr.values = append(mr.values, []sql.NamedArg{sql.Named("TableName", curDir)})
			continue
		}

		var d scheme.Directory
		err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			d, err = schemeClient.ListDirectory(ctx, curDir)
			return
		}, retry.WithIdempotent(true))

		if err != nil {
			return err
		}

		for _, child := range d.Children {
			if child.IsDirectory() || child.IsTable() {
				if canEnter(child.Name) {
					queue = append(queue, path.Join(curDir, child.Name))
				}
			}
		}
	}
	return err
}

func (mr *multiRows) getIndexes(ctx context.Context, _ string, args []driver.NamedValue) (err error) {
	margs := make(map[string]driver.Value)
	for _, arg := range args {
		margs[arg.Name] = arg.Value
	}
	if _, has := margs["TableName"]; !has {
		return fmt.Errorf("table name not found")
	}

	var (
		tableName = margs["TableName"].(string)
		cn        = mr.conn.connector.Connection()
	)

	tableExist, err := helpers.IsTableExists(ctx, cn.Scheme(), tableName)
	if err != nil {
		return err
	}
	if !tableExist {
		return fmt.Errorf("table '%s' not exist", tableName)
	}

	mr.values = make([][]sql.NamedArg, 0)

	session := mr.conn.session
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		desc, err := session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}
		for _, indexDesc := range desc.Indexes {
			mr.values = append(mr.values, []sql.NamedArg{
				sql.Named("IndexName", indexDesc.Name),
				sql.Named("Columns", strings.Join(indexDesc.IndexColumns, ",")),
			})
		}
		return
	}, retry.WithIdempotent(true))
	if err != nil {
		return err
	}

	return err
}
