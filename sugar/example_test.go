package sugar_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func Example_unmarshallRow() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources

	type myStruct struct {
		ID  int32  `sql:"id"`
		Str string `sql:"myStr"`
	}

	row, err := db.Query().QueryRow(ctx, `SELECT 42 as id, "my string" as myStr`)
	if err != nil {
		panic(err)
	}

	one, err := sugar.UnmarshallRow[*myStruct](row)
	if err != nil {
		panic(err)
	}

	fmt.Printf("one = %+v\n", one)
	// one = &{ID:42 Str:my string}
}

func Example_unmarshallResultSet() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources

	type myStruct struct {
		ID  int32  `sql:"id"`
		Str string `sql:"myStr"`
	}

	rows, err := db.Query().QueryResultSet(ctx, `
		SELECT 42 as id, "myStr42" as myStr
		UNION
		SELECT 43 as id, "myStr43" as myStr
		ORDER BY id
	`)
	if err != nil {
		panic(err)
	}

	many, err := sugar.UnmarshallResultSet[myStruct](rows)
	if err != nil {
		panic(err)
	}

	fmt.Print("many = [")
	for i, s := range many {
		if i > 0 {
			fmt.Print(",")
		}
		fmt.Printf("\n\t%+v", s)
	}
	fmt.Println("\n]")
	// many = [
	//	&{ID:42 Str:myStr42},
	//	&{ID:43 Str:myStr43}
	//]
}
