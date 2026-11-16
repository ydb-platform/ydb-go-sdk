// This example writes and reads rows in Apache Arrow format.
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func main() {
	ctx := context.Background()
	dsn := os.Getenv("YDB_CONNECTION_STRING")
	if dsn == "" {
		dsn = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(ctx, dsn, ydb.WithAnonymousCredentials())
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	const tableName = "apache_arrow_example"
	if err := db.Query().Exec(ctx, `CREATE TABLE `+tableName+` (
		id Uint64 NOT NULL,
		name Utf8 NOT NULL,
		PRIMARY KEY (id)
	);`); err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Query().Exec(ctx, "DROP TABLE "+tableName); err != nil {
			panic(err)
		}
	}()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	ids, ok := builder.Field(0).(*array.Uint64Builder)
	if !ok {
		panic("unexpected Arrow id builder")
	}
	names, ok := builder.Field(1).(*array.StringBuilder)
	if !ok {
		panic("unexpected Arrow name builder")
	}
	ids.AppendValues([]uint64{24, 42}, nil)
	names.AppendValues([]string{"WOW", "my string"}, nil)
	batch := builder.NewRecordBatch()
	defer batch.Release()

	schemaPayload := ipc.GetSchemaPayload(schema, memory.DefaultAllocator)
	defer schemaPayload.Release()
	var schemaBytes bytes.Buffer
	if _, err := schemaPayload.WritePayload(&schemaBytes); err != nil {
		panic(err)
	}
	dataPayload, err := ipc.GetRecordBatchPayload(batch)
	if err != nil {
		panic(err)
	}
	defer dataPayload.Release()
	var dataBytes bytes.Buffer
	if _, err := dataPayload.WritePayload(&dataBytes); err != nil {
		panic(err)
	}
	if err := db.Table().BulkUpsert(ctx, path.Join(db.Name(), tableName),
		table.BulkUpsertDataArrow(dataBytes.Bytes(), table.WithArrowSchema(schemaBytes.Bytes())),
	); err != nil {
		panic(err)
	}

	const sql = "SELECT id, name FROM " + tableName + " ORDER BY id;"
	err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		result, err := s.QueryArrow(ctx, sql)
		if err != nil {
			return err
		}
		defer result.Close(ctx)
		for part, err := range result.Parts(ctx) {
			if err != nil {
				return err
			}
			reader, err := ipc.NewReader(part)
			if err != nil {
				return err
			}
			for reader.Next() {
				batch := reader.RecordBatch()
				ids, ok := batch.Column(0).(*array.Uint64)
				if !ok {
					reader.Release()

					return fmt.Errorf("unexpected Arrow id column")
				}
				names, ok := batch.Column(1).(*array.String)
				if !ok {
					reader.Release()

					return fmt.Errorf("unexpected Arrow name column")
				}
				for row := 0; row < int(batch.NumRows()); row++ {
					fmt.Printf("QueryArrow: id=%d name=%q\n", ids.Value(row), names.Value(row))
				}
			}
			err = reader.Err()
			reader.Release()
			if err != nil {
				return err
			}
		}

		return nil
	}, query.WithIdempotent())
	if err != nil {
		panic(err)
	}

	err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		result, err := s.Query(ctx, sql, query.WithResultFormatArrow(ipc.NewReader))
		if err != nil {
			return err
		}
		defer result.Close(ctx)
		for rs, err := range result.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				var id uint64
				var name string
				if err := row.ScanNamed(query.Named("id", &id), query.Named("name", &name)); err != nil {
					return err
				}
				fmt.Printf("WithResultFormatArrow: id=%d name=%q\n", id, name)
			}
		}

		return nil
	}, query.WithIdempotent())
	if err != nil {
		panic(err)
	}
}
