package main

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

var (
	// https://github.com/zonination/weather-intl/blob/master/moscow.csv
	// only 10 lines of 2014 year
	//go:embed weather.csv
	weatherCSV []byte

	//nolint:lll
	// git log --date=format-local:'%Y-%m-%d %H:%M:%S' --pretty=format:'{"hash":"%h","msg":"%q","date":"%ad","author":"%ae"}' > commits.json
	// only 10 lines of 2022 year
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

	if err := createSchema(ctx, db); err != nil {
		panic(err)
	}

	if err := fillTableWeather(ctx, db); err != nil {
		panic(err)
	}

	minTemperature, avgTemperature, maxTemperature, err := getWeatherStatsFromTable(ctx, db, 2014)
	if err != nil {
		panic(err)
	}

	fmt.Printf(`Weather stats of 2014 year:
- minTemperature: %f
- avgTemperature: %f
- maxTemperature: %f
`, minTemperature, avgTemperature, maxTemperature,
	)

	if err := fillTopicCommits(ctx, db); err != nil {
		panic(err)
	}

	commits, err := getCommitStats(ctx, db, 2022)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Commits to YDB at 2022 year: %d\n", commits)
}

func createSchema(ctx context.Context, db *ydb.Driver) error {
	return db.Query().Exec(ctx, schema)
}

type commit struct {
	Hash   string `json:"hash"`
	Msg    string `json:"msg"`
	Author string `json:"author"`
	Date   string `json:"date"`
}

func (c *commit) UnmarshalYDBTopicMessage(data []byte) error {
	return json.Unmarshal(data, c)
}

func fillTopicCommits(ctx context.Context, db *ydb.Driver) error {
	writer, err := db.Topic().StartWriter("commits", topicoptions.WithWriterWaitServerAck(false))
	if err != nil {
		return err
	}
	defer writer.Close(ctx)

	scanner := bufio.NewScanner(bytes.NewReader(commitsJSON))
	for scanner.Scan() {
		content := scanner.Bytes()

		var commit commit

		err = json.Unmarshal(content, &commit)
		if err == nil {
			data, err := json.Marshal(commit)
			if err != nil {
				return err
			}

			err = writer.Write(ctx, topicwriter.Message{Data: bytes.NewReader(data)})
			if err != nil {
				return err
			}
		}
	}

	return scanner.Err()
}

func rowToRow(row []string) (types.Value, error) {
	if len(row) != 26 {
		return nil, errors.New("invalid row")
	}
	var ID uint64
	if v, err := strconv.ParseUint(row[0], 10, 64); err != nil {
		return nil, err
	} else { //nolint:revive
		ID = v
	}
	var Date *string
	if v := row[1]; v != "" {
		Date = &v
	}
	var MaxTemperatureF *int64
	if v, err := strconv.ParseInt(row[2], 10, 64); err == nil {
		MaxTemperatureF = &v
	}
	var MeanTemperatureF *int64
	if v, err := strconv.ParseInt(row[3], 10, 64); err == nil {
		MeanTemperatureF = &v
	}
	var MinTemperatureF *int64
	if v, err := strconv.ParseInt(row[4], 10, 64); err == nil {
		MinTemperatureF = &v
	}
	var MaxDewPointF *int64
	if v, err := strconv.ParseInt(row[5], 10, 64); err == nil {
		MaxDewPointF = &v
	}
	var MeanDewPointF *int64
	if v, err := strconv.ParseInt(row[6], 10, 64); err == nil {
		MeanDewPointF = &v
	}
	var MinDewPointF *int64
	if v, err := strconv.ParseInt(row[7], 10, 64); err == nil {
		MinDewPointF = &v
	}
	var MaxHumidity *int64
	if v, err := strconv.ParseInt(row[8], 10, 64); err == nil {
		MaxHumidity = &v
	}
	var MeanHumidity *int64
	if v, err := strconv.ParseInt(row[9], 10, 64); err == nil {
		MeanHumidity = &v
	}
	var MinHumidity *int64
	if v, err := strconv.ParseInt(row[10], 10, 64); err == nil {
		MinHumidity = &v
	}
	var MaxSeaLevelPressureIn *float64
	if v, err := strconv.ParseFloat(row[11], 64); err == nil {
		MaxSeaLevelPressureIn = &v
	}
	var MeanSeaLevelPressureIn *float64
	if v, err := strconv.ParseFloat(row[12], 64); err == nil {
		MeanSeaLevelPressureIn = &v
	}
	var MinSeaLevelPressureIn *float64
	if v, err := strconv.ParseFloat(row[13], 64); err == nil {
		MinSeaLevelPressureIn = &v
	}
	var MaxVisibilityMiles *int64
	if v, err := strconv.ParseInt(row[14], 10, 64); err == nil {
		MaxVisibilityMiles = &v
	}
	var MeanVisibilityMiles *int64
	if v, err := strconv.ParseInt(row[15], 10, 64); err == nil {
		MeanVisibilityMiles = &v
	}
	var MinVisibilityMiles *int64
	if v, err := strconv.ParseInt(row[16], 10, 64); err == nil {
		MinVisibilityMiles = &v
	}
	var MaxWindSpeedMPH *int64
	if v, err := strconv.ParseInt(row[17], 10, 64); err == nil {
		MaxWindSpeedMPH = &v
	}
	var MeanWindSpeedMPH *int64
	if v, err := strconv.ParseInt(row[18], 10, 64); err == nil {
		MeanWindSpeedMPH = &v
	}
	var MaxGustSpeedMPH *int64
	if v, err := strconv.ParseInt(row[19], 10, 64); err == nil {
		MaxGustSpeedMPH = &v
	}
	var PrecipitationIn *float64
	if v, err := strconv.ParseFloat(row[20], 64); err == nil {
		PrecipitationIn = &v
	}
	var CloudCover *int64
	if v, err := strconv.ParseInt(row[21], 10, 64); err == nil {
		CloudCover = &v
	}
	var Events *string
	if v := row[22]; v != "" {
		Events = &v
	}
	var WindDirDegrees *string
	if v := row[23]; v != "" {
		WindDirDegrees = &v
	}
	var city *string
	if v := row[24]; v != "" {
		city = &v
	}
	var season *string
	if v := row[25]; v != "" {
		season = &v
	}

	return types.StructValue(
		types.StructFieldValue("ID", types.Uint64Value(ID)),
		types.StructFieldValue("Date", types.NullableTextValue(Date)),
		types.StructFieldValue("MaxTemperatureF", types.NullableInt64Value(MaxTemperatureF)),
		types.StructFieldValue("MeanTemperatureF", types.NullableInt64Value(MeanTemperatureF)),
		types.StructFieldValue("MinTemperatureF", types.NullableInt64Value(MinTemperatureF)),
		types.StructFieldValue("MaxDewPointF", types.NullableInt64Value(MaxDewPointF)),
		types.StructFieldValue("MeanDewPointF", types.NullableInt64Value(MeanDewPointF)),
		types.StructFieldValue("MinDewPointF", types.NullableInt64Value(MinDewPointF)),
		types.StructFieldValue("MaxHumidity", types.NullableInt64Value(MaxHumidity)),
		types.StructFieldValue("MeanHumidity", types.NullableInt64Value(MeanHumidity)),
		types.StructFieldValue("MinHumidity", types.NullableInt64Value(MinHumidity)),
		types.StructFieldValue("MaxSeaLevelPressureIn", types.NullableDoubleValue(MaxSeaLevelPressureIn)),
		types.StructFieldValue("MeanSeaLevelPressureIn", types.NullableDoubleValue(MeanSeaLevelPressureIn)),
		types.StructFieldValue("MinSeaLevelPressureIn", types.NullableDoubleValue(MinSeaLevelPressureIn)),
		types.StructFieldValue("MaxVisibilityMiles", types.NullableInt64Value(MaxVisibilityMiles)),
		types.StructFieldValue("MeanVisibilityMiles", types.NullableInt64Value(MeanVisibilityMiles)),
		types.StructFieldValue("MinVisibilityMiles", types.NullableInt64Value(MinVisibilityMiles)),
		types.StructFieldValue("MaxWindSpeedMPH", types.NullableInt64Value(MaxWindSpeedMPH)),
		types.StructFieldValue("MeanWindSpeedMPH", types.NullableInt64Value(MeanWindSpeedMPH)),
		types.StructFieldValue("MaxGustSpeedMPH", types.NullableInt64Value(MaxGustSpeedMPH)),
		types.StructFieldValue("PrecipitationIn", types.NullableDoubleValue(PrecipitationIn)),
		types.StructFieldValue("CloudCover", types.NullableInt64Value(CloudCover)),
		types.StructFieldValue("Events", types.NullableTextValue(Events)),
		types.StructFieldValue("WindDirDegrees", types.NullableTextValue(WindDirDegrees)),
		types.StructFieldValue("city", types.NullableTextValue(city)),
		types.StructFieldValue("season", types.NullableTextValue(season)),
	), nil
}

func fillTableWeather(ctx context.Context, db *ydb.Driver) error {
	csvReader := csv.NewReader(bytes.NewReader(weatherCSV))
	var values []types.Value
	for {
		row, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		value, err := rowToRow(row)
		if err != nil {
			return err
		}

		values = append(values, value)
	}

	return db.Table().BulkUpsert(ctx, path.Join(db.Name(), "weather"),
		table.BulkUpsertDataRows(types.ListValue(values...)),
	)
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

func getCommitStats(ctx context.Context, db *ydb.Driver, year int) (commits int64, _ error) {
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
				return commits, nil
			}

			return commits, err
		}

		var commit commit
		if err := msg.UnmarshalTo(&commit); err != nil {
			return commits, err
		}

		date, err := time.Parse("2006-01-02 15:04:05", commit.Date)
		if err != nil {
			return commits, err
		}

		if date.Year() == year {
			commits++
		}
	}
}
