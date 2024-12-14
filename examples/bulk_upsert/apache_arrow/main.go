package main

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/csv"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"io"
	"os"
	"path"
	"strconv"
	"time"
)

var (
	// https://github.com/zonination/weather-intl/blob/master/moscow.csv
	//go:embed weather.csv
	weatherCSV []byte

	// git log --date=format-local:'%Y-%m-%d %H:%M:%S' --pretty=format:'{"hash":"%h","msg":"%q","date":"%ad","author":"%ae"}' > commits.json
	//go:embed commits.json
	commitsJSON []byte

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

	createSchema(ctx, db)

	fillTableWeather(ctx, db)

	minTemperature, avgTemperature, maxTemperature, err := getWeatherStatsFromTable(ctx, db, 2014)
	if err != nil {
		panic(err)
	}

	fmt.Println(minTemperature, avgTemperature, maxTemperature)

	fillTopicCommits(ctx, db)

	commits := getCommitStats(ctx, db, 2022)

	fmt.Println(commits)
}

func createSchema(ctx context.Context, db *ydb.Driver) {
	err := db.Query().Exec(ctx, schema)
	if err != nil {
		panic(err)
	}
}

type commit struct {
	Hash   string `json:"hash"`
	Msg    string `json:"msg"`
	Author string `json:"author"`
	Date   string `json:"date"`
}

func fillTopicCommits(ctx context.Context, db *ydb.Driver) {
	writer, err := db.Topic().StartWriter("commits", topicoptions.WithWriterWaitServerAck(false))
	if err != nil {
		panic(err)
	}
	defer writer.Close(ctx)

	scanner := bufio.NewScanner(bytes.NewReader(commitsJSON))
	messages := make([]topicwriter.Message, 0, 1000)
	n := 0
	commits := 0
	for scanner.Scan() {
		content := scanner.Bytes()

		var commit commit

		err = json.Unmarshal(content, &commit)
		if err == nil {
			data, err := json.Marshal(commit)
			if err != nil {
				panic(err)
			}

			messages = append(messages, topicwriter.Message{Data: bytes.NewReader(data)})

			date, err := time.Parse("2006-01-02 15:04:05", commit.Date)
			if err != nil {
				panic(err)
			}

			if date.Year() == 2022 {
				commits++
			}

			n++
			if n%1000 == 0 {
				err = writer.Write(ctx, messages...)
				if err != nil {
					panic(err)
				}
				messages = messages[:0]
			}
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	fmt.Printf("commits: %d\n", commits)
}

func fillTableWeather(ctx context.Context, db *ydb.Driver) {
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
		bytes.NewBuffer(weatherCSV),
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

		err = db.Table().BulkUpsert(ctx, path.Join(db.Name(), "weather"),
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

func getWeatherStatsFromTable(ctx context.Context, db *ydb.Driver, year int) (
	minTemperature float64,
	avgTemperature float64,
	maxTemperature float64,
	_ error,
) {
	row, err := db.Query().QueryRow(ctx, `
		SELECT
		    MIN(MinTemperatureF),
		    AVG(MeanTemperatureF),
		    MAX(MaxTemperatureF)
		FROM weather
		WHERE 
    		CAST(Date AS Date) BETWEEN Date("`+strconv.Itoa(year)+`-01-01") AND Date("`+strconv.Itoa(year)+`-12-31")
		AND
    		Events LIKE "%Snow%"
	`)
	if err != nil {
		return minTemperature, avgTemperature, maxTemperature, err
	}

	err = row.Scan(&minTemperature, &avgTemperature, &maxTemperature)
	if err != nil {
		return minTemperature, avgTemperature, maxTemperature, err
	}

	return minTemperature, avgTemperature, maxTemperature, nil
}

func getCommitStats(ctx context.Context, db *ydb.Driver, year int) (commits int64) {
	_ = db.Topic().Alter(ctx, "commits", topicoptions.AlterWithDropConsumers("consumer"))

	err := db.Topic().Alter(ctx, "commits", topicoptions.AlterWithAddConsumers(topictypes.Consumer{
		Name: "consumer",
	}))
	if err != nil {
		panic(err)
	}

	reader, err := db.Topic().StartReader("consumer", topicoptions.ReadTopic("commits"))
	if err != nil {
		panic(err)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()

	for {
		msg, err := reader.ReadMessage(waitCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return commits
			}

			panic(err)
		}

		content, err := io.ReadAll(msg)
		if err != nil {
			return
		}

		var commit commit

		err = json.Unmarshal(content, &commit)
		if err == nil {
			date, err := time.Parse("2006-01-02 15:04:05", commit.Date)
			if err != nil {
				panic(err)
			}

			if date.Year() == year {
				commits++
			}
		} else {
			fmt.Println(string(content))

			panic(err)
		}
	}

	return commits
}
