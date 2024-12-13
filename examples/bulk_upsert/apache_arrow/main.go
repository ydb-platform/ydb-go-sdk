package main

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/csv"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"os"
	"path"
)

var (
	//go:embed moscow.csv
	moscowWeather []byte

	//go:embed schema.sql
	schema string
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		panic(err)
	}

	defer db.Close(ctx)

	err = db.Query().Exec(ctx, schema)
	if err != nil {
		panic(err)
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "ID", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "Date", Type: arrow.BinaryTypes.String},
			{Name: "MaxTemperatureF", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MeanTemperatureF", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MinTemperatureF", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MaxDewPointF", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MeanDewPointF", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MinDewpointF", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MaxHumidity", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MeanHumidity", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MinHumidity", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MaxSeaLevelPressureIn", Type: arrow.PrimitiveTypes.Float64},
			{Name: "MeanSeaLevelPressureIn", Type: arrow.PrimitiveTypes.Float64},
			{Name: "MinSeaLevelPressureIn", Type: arrow.PrimitiveTypes.Float64},
			{Name: "MaxVisibilityMiles", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MeanVisibilityMiles", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MinVisibilityMiles", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MaxWindSpeedMPH", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MeanWindSpeedMPH", Type: arrow.PrimitiveTypes.Int64},
			{Name: "MaxGustSpeedMPH", Type: arrow.PrimitiveTypes.Int64},
			{Name: "PrecipitationIn", Type: arrow.PrimitiveTypes.Float64},
			{Name: "CloudCover", Type: arrow.PrimitiveTypes.Int64},
			{Name: "Events", Type: arrow.BinaryTypes.String},
			{Name: "WindDirDegrees", Type: arrow.BinaryTypes.String},
			{Name: "city", Type: arrow.BinaryTypes.String},
			{Name: "season", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	r := csv.NewReader(
		bytes.NewBuffer(moscowWeather),
		schema,
		csv.WithComma(','),
		csv.WithLazyQuotes(true),
		csv.WithHeader(false),
		csv.WithNullReader(true, ""),
	)
	defer r.Release()

	s, err := schemaBytes(schema)
	if err != nil {
		panic(err)
	}

	var data bytes.Buffer
	writer := ipc.NewWriter(&data, ipc.WithSchema(schema))

	for r.Next() {
		err = writer.Write(r.Record())
		if err != nil {
			panic(err)
		}

		d := bytes.TrimPrefix(data.Bytes(), s)

		err = db.Table().BulkUpsert(ctx, path.Join(db.Name(), "moscow_weather"),
			table.BulkUpsertDataArrow(d, table.WithArrowSchema(s)),
		)
		if err != nil {
			panic(err)
		}

		data.Reset()
	}

	if err = r.Err(); err != nil {
		panic(err)
	}
}

// https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
var arrowIPCEndOfStreamMark = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0}

func schemaBytes(s *arrow.Schema) (schema []byte, err error) {
	buf := &bytes.Buffer{}
	writer := ipc.NewWriter(buf, ipc.WithSchema(s))

	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to save arrow schema: %w", err)
	}

	schema = bytes.Clone(buf.Bytes())
	schema = bytes.TrimSuffix(schema, arrowIPCEndOfStreamMark)

	return schema, nil
}
