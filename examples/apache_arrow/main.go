// This example shows how you can get results in Apache Arrow format.
//
// Before starting add apache arrow IPC package:
//
//	go get github.com/apache/arrow/go/arrow
package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func main() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources

	sql := `SELECT 42 as id, "my string" as myStr;
SELECT 24 as id, "WOW" as myStr, "UHH" as secondStr;`

	result, err := db.Query().QueryArrow(ctx, sql, query.WithIdempotent())
	if err != nil {
		panic(err)
	}
	defer result.Close(ctx)

	for part, err := range result.Parts(ctx) {
		if err != nil {
			panic(err)
		}

		fmt.Printf("ResultSet#%d ", part.GetResultSetIndex())

		// or you can use `part.Bytes()` instead of `io.Reader` interface

		rdr, err := ipc.NewReader(part) // part already implements io.Reader
		if err != nil {
			panic(err)
		}

		for rdr.Next() {
			out := rdr.Record()
			fmt.Println(out)
		}
	}

	// Output:
	// ResultSet#0 record:
	//
	//	schema:
	//	fields: 2
	//	  - id: type=int32
	//	  - myStr: type=binary
	//	rows: 1
	//	col[0][id]: [42]
	//	col[1][myStr]: ["my string"]
	//
	// ResultSet#1 record:
	//
	//	schema:
	//	fields: 3
	//	  - id: type=int32
	//	  - myStr: type=binary
	//	  - secondStr: type=binary
	//	rows: 1
	//	col[0][id]: [24]
	//	col[1][myStr]: ["WOW"]
	//	col[2][secondStr]: ["UHH"]
}
