package resultset

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
)

// Result is a resultset of a query.
//
// Use NextResultSet(), NextRow() and Scan() to advance through the resultset sets,
// its rows and row's items.
//
//     res, err := s.Execute(ctx, txc, "SELECT ...")
//     defer res.Close()
//     for res.NextResultSet() {
//         for res.NextRow() {
//             var id int64
//             var name *string //optional value
//             res.Scan(&id,&name)
//         }
//     }
//     if err := res.Err() { // get any error encountered during iteration
//         // handle error
//     }
//
// If current value under scan
// is not requested types, then res.Err() become non-nil.
// After that, NextResultSet(), NextRow() will return false.
type Result interface {
	ResultSetCount() int
	TotalRowCount() int
	HasNextResultSet() bool
	NextResultSet(ctx context.Context, columns ...string) bool
	CurrentResultSet() scanner.ResultSet
	HasNextRow() bool
	NextRow() bool
	ScanWithDefaults(values ...interface{}) error
	Scan(values ...interface{}) error
	Stats() (s scanner.QueryStats)
	Err() error
	Close() error
}

var _ Result = &scanner.Result{}
