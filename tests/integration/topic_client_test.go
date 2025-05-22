//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

const defaultConnectionString = "grpc://localhost:2136/local"

const commonConsumerName = "consumer"

func TestTopicCreateDrop(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)
	topicPath := db.Name() + "/testtopic"

	_ = db.Topic().Drop(ctx, topicPath)
	err := db.Topic().Create(ctx, topicPath,
		topicoptions.CreateWithConsumer(
			topictypes.Consumer{
				Name: "test",
			},
		),
	)
	require.NoError(t, err)

	_, err = db.Topic().Describe(ctx, topicPath)
	require.NoError(t, err)

	err = db.Topic().Drop(ctx, topicPath)
	require.NoError(t, err)
}

func TestTopicDescribe(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)
	topicName := "test-topic-" + t.Name()

	var (
		supportedCodecs     = []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip}
		minActivePartitions = int64(2)
		// partitionCountLimit = int64(5) LOGBROKER-7800
		retentionPeriod = time.Hour
		writeSpeed      = int64(1023)
		burstBytes      = int64(222)
		consumers       = []topictypes.Consumer{
			{
				Name:            "c1",
				Important:       false,
				SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
				ReadFrom:        time.Date(2022, 9, 11, 10, 1, 2, 0, time.UTC),
			},
			{
				Name:            "c2",
				SupportedCodecs: []topictypes.Codec{},
				ReadFrom:        time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC),
			},
		}
	)

	_ = db.Topic().Drop(ctx, topicName)
	err := db.Topic().Create(ctx, topicName,
		topicoptions.CreateWithSupportedCodecs(supportedCodecs...),
		topicoptions.CreateWithMinActivePartitions(minActivePartitions),
		// topicoptions.CreateWithPartitionCountLimit(partitionCountLimit), LOGBROKER-7800
		topicoptions.CreateWithRetentionPeriod(retentionPeriod),
		// topicoptions.CreateWithRetentionStorageMB(...) - incompatible with retention period
		topicoptions.CreateWithPartitionWriteSpeedBytesPerSecond(writeSpeed),
		topicoptions.CreateWithPartitionWriteBurstBytes(burstBytes),
		topicoptions.CreateWithConsumer(consumers...),
		// topicoptions.CreateWithMeteringMode(topictypes.MeteringModeRequestUnits), - work with serverless only
	)
	require.NoError(t, err)

	topicDesc, err := db.Topic().Describe(ctx, topicName)
	require.NoError(t, err)

	expected := topictypes.TopicDescription{
		Path: topicName,
		PartitionSettings: topictypes.PartitionSettings{
			MinActivePartitions: minActivePartitions,
			// PartitionCountLimit: partitionCountLimit, LOGBROKER-7800
		},
		Partitions: []topictypes.PartitionInfo{
			{
				PartitionID: 0,
				Active:      true,
			},
			{
				PartitionID: 1,
				Active:      true,
			},
		},
		RetentionPeriod:                   retentionPeriod,
		RetentionStorageMB:                0,
		SupportedCodecs:                   supportedCodecs,
		PartitionWriteBurstBytes:          burstBytes,
		PartitionWriteSpeedBytesPerSecond: writeSpeed,
		Attributes:                        nil,
		Consumers:                         consumers,
		MeteringMode:                      topictypes.MeteringModeUnspecified,
	}

	requireAndCleanSubset := func(checked *map[string]string, subset *map[string]string) {
		t.Helper()
		for k, subValue := range *subset {
			checkedValue, ok := (*checked)[k]
			require.True(t, ok, k)
			require.Equal(t, subValue, checkedValue)
		}
		*checked = nil
		*subset = nil
	}

	requireAndCleanSubset(&topicDesc.Attributes, &expected.Attributes)

	for i := range expected.Consumers {
		requireAndCleanSubset(&topicDesc.Consumers[i].Attributes, &expected.Consumers[i].Attributes)
	}

	require.Equal(t, expected, topicDesc)
}

func TestTopicDescribePartitionStats(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)
	topicName := "test-topic-" + t.Name()

	var (
		supportedCodecs     = []topictypes.Codec{topictypes.CodecRaw}
		minActivePartitions = int64(1)
	)

	_ = db.Topic().Drop(ctx, topicName)
	err := db.Topic().Create(ctx, topicName,
		topicoptions.CreateWithSupportedCodecs(supportedCodecs...),
		topicoptions.CreateWithMinActivePartitions(minActivePartitions),
	)
	require.NoError(t, err)

	writer, err := db.Topic().StartWriter(topicName, topicoptions.WithWriterWaitServerAck(true))
	require.NoError(t, err)
	topicMessage := topicwriter.Message{
		Data: bytes.NewReader([]byte{128}),
	}
	err = writer.Write(ctx, topicMessage)
	require.NoError(t, err)

	topicDesc, err := db.Topic().Describe(ctx, topicName, topicoptions.IncludePartitionStats())
	require.NoError(t, err)

	expected := topictypes.TopicDescription{
		Path: topicName,
		PartitionSettings: topictypes.PartitionSettings{
			MinActivePartitions: minActivePartitions,
		},
		Partitions: []topictypes.PartitionInfo{
			{
				PartitionID: 0,
				Active:      true,
				PartitionStats: topictypes.PartitionStats{
					PartitionsOffset: topictypes.OffsetRange{
						Start: 0,
						End:   1,
					},
					BytesWritten: topictypes.MultipleWindowsStat{
						PerMinute: 1,
						PerHour:   1,
						PerDay:    1,
					},
				},
			},
		},
		RetentionPeriod:                   86400000000000,
		RetentionStorageMB:                0,
		SupportedCodecs:                   supportedCodecs,
		PartitionWriteBurstBytes:          1048576,
		PartitionWriteSpeedBytesPerSecond: 1048576,
		Attributes:                        nil,
		Consumers:                         []topictypes.Consumer{},
		MeteringMode:                      topictypes.MeteringModeUnspecified,
	}

	requireAndCleanSubset := func(checked *map[string]string, subset *map[string]string) {
		t.Helper()
		for k, subValue := range *subset {
			checkedValue, ok := (*checked)[k]
			require.True(t, ok, k)
			require.Equal(t, subValue, checkedValue)
		}
		*checked = nil
		*subset = nil
	}

	topicDesc.Partitions[0].PartitionStats.LastWriteTime = nil
	bw := &topicDesc.Partitions[0].PartitionStats.BytesWritten
	sign := func(x int64) int64 {
		if x > 0 {
			return 1
		}
		return 0
	}
	topicDesc.Partitions[0].PartitionStats.BytesWritten = topictypes.MultipleWindowsStat{
		PerMinute: sign(bw.PerMinute),
		PerHour:   sign(bw.PerHour),
		PerDay:    sign(bw.PerDay),
	}

	requireAndCleanSubset(&topicDesc.Attributes, &expected.Attributes)
	require.Equal(t, expected, topicDesc)
}

func TestDescribePartitionSettings(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	const (
		defaultRetentionPeriod = 24 * time.Hour
		writeSpeed             = int64(1024)
		burstBytes             = int64(2048)
	)

	var (
		supportedCodecs          = []topictypes.Codec{topictypes.CodecRaw}
		minActivePartitions      = int64(2)
		maxActivePartitions      = int64(5)
		topicName                = "test-topic-" + t.Name()
		topicPath                = scope.Driver().Name() + "/" + topicName
		autoPartitioningSettings = topictypes.AutoPartitioningSettings{
			AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyScaleUp,
			AutoPartitioningWriteSpeedStrategy: topictypes.AutoPartitioningWriteSpeedStrategy{
				StabilizationWindow:    10 * time.Second,
				UpUtilizationPercent:   80,
				DownUtilizationPercent: 20,
			},
		}
	)

	_ = scope.Driver().Topic().Drop(ctx, topicPath)
	err := scope.Driver().Topic().Create(ctx, topicPath,
		topicoptions.CreateWithSupportedCodecs(supportedCodecs...),
		topicoptions.CreateWithMinActivePartitions(minActivePartitions),
		topicoptions.CreateWithMaxActivePartitions(maxActivePartitions),
		topicoptions.CreateWithAutoPartitioningSettings(autoPartitioningSettings),
		topicoptions.CreateWithPartitionWriteSpeedBytesPerSecond(writeSpeed),
		topicoptions.CreateWithPartitionWriteBurstBytes(burstBytes),
	)
	require.NoError(t, err)

	topicDesc, err := scope.Driver().Topic().Describe(ctx, topicPath)
	require.NoError(t, err)

	expected := topictypes.TopicDescription{
		Path: topicName,
		PartitionSettings: topictypes.PartitionSettings{
			MinActivePartitions:      minActivePartitions,
			MaxActivePartitions:      maxActivePartitions,
			AutoPartitioningSettings: autoPartitioningSettings,
		},
		Partitions: []topictypes.PartitionInfo{
			{
				PartitionID: 0,
				Active:      true,
			},
			{
				PartitionID: 1,
				Active:      true,
			},
		},
		RetentionPeriod:                   defaultRetentionPeriod,
		RetentionStorageMB:                0,
		SupportedCodecs:                   supportedCodecs,
		PartitionWriteBurstBytes:          burstBytes,
		PartitionWriteSpeedBytesPerSecond: writeSpeed,
		Attributes:                        nil,
		Consumers:                         []topictypes.Consumer{},
		MeteringMode:                      topictypes.MeteringModeUnspecified,
	}

	requireAndCleanSubset := func(checked *map[string]string, subset *map[string]string) {
		t.Helper()
		for k, subValue := range *subset {
			checkedValue, ok := (*checked)[k]
			require.True(t, ok, k)
			require.Equal(t, subValue, checkedValue)
		}
		*checked = nil
		*subset = nil
	}

	requireAndCleanSubset(&topicDesc.Attributes, &expected.Attributes)
	require.Equal(t, expected, topicDesc)
}

func TestDescribeTopicConsumer(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)
	topicName := "test-topic-" + t.Name()
	var (
		supportedCodecs     = []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip}
		minActivePartitions = int64(2)
		consumers           = []topictypes.Consumer{
			{
				Name:            "c1",
				Important:       true,
				SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
				ReadFrom:        time.Date(2022, 9, 11, 10, 1, 2, 0, time.UTC),
			},
			{
				Name:            "c2",
				Important:       false,
				SupportedCodecs: []topictypes.Codec{},
				ReadFrom:        time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC),
			},
		}
	)

	_ = db.Topic().Drop(ctx, topicName)
	err := db.Topic().Create(ctx, topicName,
		topicoptions.CreateWithSupportedCodecs(supportedCodecs...),
		topicoptions.CreateWithMinActivePartitions(minActivePartitions),
		topicoptions.CreateWithConsumer(consumers...),
	)
	require.NoError(t, err)

	consumer, err := db.Topic().DescribeTopicConsumer(ctx, topicName, "c1", topicoptions.IncludeConsumerStats())
	require.NoError(t, err)

	zeroTime := time.Time{}
	zeroDuration := time.Duration(0)
	expectedConsumerDesc := topictypes.TopicConsumerDescription{
		Path: path.Join(topicName, "c1"),
		Consumer: topictypes.Consumer{
			Name:            "c1",
			Important:       true,
			SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
			ReadFrom:        time.Date(2022, 9, 11, 10, 1, 2, 0, time.UTC),
			Attributes: map[string]string{
				"_service_type": "data-streams",
			},
		},
		Partitions: []topictypes.DescribeConsumerPartitionInfo{
			{
				PartitionID: 0,
				Active:      true,
				PartitionStats: topictypes.PartitionStats{
					PartitionsOffset: topictypes.OffsetRange{},
					StoreSizeBytes:   0,
					LastWriteTime:    nil,
					MaxWriteTimeLag:  &zeroDuration,
					BytesWritten:     topictypes.MultipleWindowsStat{},
				},
				PartitionConsumerStats: topictypes.PartitionConsumerStats{
					LastReadOffset:                 0,
					CommittedOffset:                0,
					ReadSessionID:                  "",
					PartitionReadSessionCreateTime: &zeroTime,
					LastReadTime:                   &zeroTime,
					MaxReadTimeLag:                 &zeroDuration,
					MaxWriteTimeLag:                &zeroDuration,
					BytesRead:                      topictypes.MultipleWindowsStat{},
					ReaderName:                     "",
				},
			},
			{
				PartitionID: 1,
				Active:      true,
				PartitionStats: topictypes.PartitionStats{
					PartitionsOffset: topictypes.OffsetRange{},
					StoreSizeBytes:   0,
					LastWriteTime:    nil,
					MaxWriteTimeLag:  &zeroDuration,
					BytesWritten:     topictypes.MultipleWindowsStat{},
				},
				PartitionConsumerStats: topictypes.PartitionConsumerStats{
					LastReadOffset:                 0,
					CommittedOffset:                0,
					ReadSessionID:                  "",
					PartitionReadSessionCreateTime: &zeroTime,
					LastReadTime:                   &zeroTime,
					MaxReadTimeLag:                 &zeroDuration,
					MaxWriteTimeLag:                &zeroDuration,
					BytesRead:                      topictypes.MultipleWindowsStat{},
					ReaderName:                     "",
				},
			},
		},
	}

	requireAndCleanSubset := func(checked *map[string]string, subset *map[string]string) {
		t.Helper()
		for k, subValue := range *subset {
			checkedValue, ok := (*checked)[k]
			require.True(t, ok, k)
			require.Equal(t, subValue, checkedValue)
		}
		*checked = nil
		*subset = nil
	}

	requireAndCleanSubset(&consumer.Consumer.Attributes, &expectedConsumerDesc.Consumer.Attributes)

	for i := range consumer.Partitions {
		// Fields that are checked here are dynamic and they change with time, so we need to set them to expected values
		// to make the comparison possible.
		p := &consumer.Partitions[i]

		require.NotNil(t, p.PartitionStats.LastWriteTime)

		p.PartitionStats.LastWriteTime = expectedConsumerDesc.Partitions[i].PartitionStats.LastWriteTime
		p.PartitionStats.MaxWriteTimeLag = expectedConsumerDesc.Partitions[i].PartitionStats.MaxWriteTimeLag

		require.NotNil(t, p.PartitionConsumerStats.LastReadTime)
		require.NotNil(t, p.PartitionConsumerStats.MaxReadTimeLag)
		require.NotNil(t, p.PartitionConsumerStats.MaxWriteTimeLag)

		p.PartitionConsumerStats.PartitionReadSessionCreateTime = expectedConsumerDesc.Partitions[i].PartitionConsumerStats.PartitionReadSessionCreateTime
		p.PartitionConsumerStats.LastReadTime = expectedConsumerDesc.Partitions[i].PartitionConsumerStats.LastReadTime
		p.PartitionConsumerStats.MaxReadTimeLag = expectedConsumerDesc.Partitions[i].PartitionConsumerStats.MaxReadTimeLag
		p.PartitionConsumerStats.MaxWriteTimeLag = expectedConsumerDesc.Partitions[i].PartitionConsumerStats.MaxWriteTimeLag
	}

	require.Equal(t, expectedConsumerDesc, consumer)
}

func TestSchemeList(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)

	topicPath := createTopic(ctx, t, db)
	list, err := db.Scheme().ListDirectory(ctx, db.Name())
	require.NoError(t, err)

	topicName := path.Base(topicPath)

	hasTopic := false
	for _, e := range list.Children {
		if e.IsTopic() && topicName == e.Name {
			hasTopic = true
		}
	}
	require.True(t, hasTopic)
}

func TestReaderWithoutConsumer(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		scope := newScope(t)
		ctx := scope.Ctx

		reader1, err := scope.Driver().Topic().StartReader(
			"",
			topicoptions.ReadSelectors{
				{
					Path:       scope.TopicPath(),
					Partitions: []int64{0},
				},
			},
			topicoptions.WithReaderWithoutConsumer(false),
		)
		require.NoError(t, err)

		reader2, err := scope.Driver().Topic().StartReader(
			"",
			topicoptions.ReadSelectors{
				{
					Path:       scope.TopicPath(),
					Partitions: []int64{0},
				},
			},
			topicoptions.WithReaderWithoutConsumer(false),
		)
		require.NoError(t, err)

		err = scope.TopicWriter().Write(ctx, topicwriter.Message{Data: strings.NewReader("123")})
		require.NoError(t, err)

		msg1, err := reader1.ReadMessage(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), msg1.SeqNo)

		msg1data, err := io.ReadAll(msg1)
		require.NoError(t, err)
		require.Equal(t, "123", string(msg1data))

		msg2, err := reader2.ReadMessage(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), msg2.SeqNo)

		msg2data, err := io.ReadAll(msg2)
		require.NoError(t, err)
		require.Equal(t, "123", string(msg2data))

		_ = reader1.Close(ctx)
		_ = reader2.Close(ctx)
	})
	t.Run("NoNameNoOptionErr", func(t *testing.T) {
		scope := newScope(t)
		topicReader, err := scope.Driver().Topic().StartReader("", topicoptions.ReadTopic(scope.TopicPath()))
		require.Error(t, err)
		require.Nil(t, topicReader)
	})
	t.Run("NameAndOption", func(t *testing.T) {
		scope := newScope(t)
		topicReader, err := scope.Driver().Topic().StartReader(
			scope.TopicConsumerName(),
			topicoptions.ReadTopic(scope.TopicPath()),
			topicoptions.WithReaderWithoutConsumer(false),
		)
		require.Error(t, err)
		require.Nil(t, topicReader)
	})
}

func TestAlterTopic(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	const (
		defaultRetentionPeriod = 24 * time.Hour
		writeSpeed             = int64(1024)
		burstBytes             = int64(2048)
	)

	var (
		supportedCodecs     = []topictypes.Codec{topictypes.CodecRaw}
		minActivePartitions = int64(2)
		maxActivePartitions = int64(5)
		topicName           = "test-topic-" + t.Name()
		topicPath           = scope.Driver().Name() + "/" + topicName
	)

	_ = scope.Driver().Topic().Drop(ctx, topicPath)
	err := scope.Driver().Topic().Create(ctx, topicPath,
		topicoptions.CreateWithSupportedCodecs(supportedCodecs...),
		topicoptions.CreateWithMinActivePartitions(minActivePartitions),
		topicoptions.CreateWithMaxActivePartitions(maxActivePartitions),
		topicoptions.CreateWithPartitionWriteSpeedBytesPerSecond(writeSpeed),
		topicoptions.CreateWithPartitionWriteBurstBytes(burstBytes),
	)
	require.NoError(t, err)

	// First alter: Change auto partitioning settings
	err = scope.Driver().Topic().Alter(ctx, topicPath,
		topicoptions.AlterWithAutoPartitioningStrategy(topictypes.AutoPartitioningStrategyScaleUp),
		topicoptions.AlterWithAutoPartitioningWriteSpeedStabilizationWindow(10*time.Second),
		topicoptions.AlterWithAutoPartitioningWriteSpeedUpUtilizationPercent(80),
		topicoptions.AlterWithAutoPartitioningWriteSpeedDownUtilizationPercent(20),
	)
	require.NoError(t, err)

	// Second alter: Change retention period and supported codecs
	newRetentionPeriod := 48 * time.Hour
	newSupportedCodecs := []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip}
	err = scope.Driver().Topic().Alter(ctx, topicPath,
		topicoptions.AlterWithRetentionPeriod(newRetentionPeriod),
		topicoptions.AlterWithSupportedCodecs(newSupportedCodecs...),
	)
	require.NoError(t, err)

	// Third alter: Change write speed and burst bytes
	newWriteSpeed := int64(2048)
	newBurstBytes := int64(4096)
	err = scope.Driver().Topic().Alter(ctx, topicPath,
		topicoptions.AlterWithPartitionWriteSpeedBytesPerSecond(newWriteSpeed),
		topicoptions.AlterWithPartitionWriteBurstBytes(newBurstBytes),
	)
	require.NoError(t, err)

	// Verify all changes
	topicDesc, err := scope.Driver().Topic().Describe(ctx, topicPath)
	require.NoError(t, err)

	expected := topictypes.TopicDescription{
		Path: topicName,
		PartitionSettings: topictypes.PartitionSettings{
			MinActivePartitions: minActivePartitions,
			MaxActivePartitions: maxActivePartitions,
			AutoPartitioningSettings: topictypes.AutoPartitioningSettings{
				AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyScaleUp,
				AutoPartitioningWriteSpeedStrategy: topictypes.AutoPartitioningWriteSpeedStrategy{
					StabilizationWindow:    10 * time.Second,
					UpUtilizationPercent:   80,
					DownUtilizationPercent: 20,
				},
			},
		},
		Partitions: []topictypes.PartitionInfo{
			{
				PartitionID: 0,
				Active:      true,
			},
			{
				PartitionID: 1,
				Active:      true,
			},
		},
		RetentionPeriod:                   newRetentionPeriod,
		RetentionStorageMB:                0,
		SupportedCodecs:                   newSupportedCodecs,
		PartitionWriteBurstBytes:          newBurstBytes,
		PartitionWriteSpeedBytesPerSecond: newWriteSpeed,
		Attributes:                        nil,
		Consumers:                         []topictypes.Consumer{},
		MeteringMode:                      topictypes.MeteringModeUnspecified,
	}

	requireAndCleanSubset := func(checked *map[string]string, subset *map[string]string) {
		t.Helper()
		for k, subValue := range *subset {
			checkedValue, ok := (*checked)[k]
			require.True(t, ok, k)
			require.Equal(t, subValue, checkedValue)
		}
		*checked = nil
		*subset = nil
	}

	requireAndCleanSubset(&topicDesc.Attributes, &expected.Attributes)
	require.Equal(t, expected, topicDesc)

	// Test altering max active partitions
	newMaxActivePartitions := int64(10)
	err = scope.Driver().Topic().Alter(ctx, topicPath,
		topicoptions.AlterWithMaxActivePartitions(newMaxActivePartitions),
	)
	require.NoError(t, err)

	// Verify max active partitions
	topicDesc, err = scope.Driver().Topic().Describe(ctx, topicPath)
	require.NoError(t, err)
	require.Equal(t, newMaxActivePartitions, topicDesc.PartitionSettings.MaxActivePartitions)

	// Test altering min active partitions
	newMinActivePartitions := int64(3)
	err = scope.Driver().Topic().Alter(ctx, topicPath,
		topicoptions.AlterWithMinActivePartitions(newMinActivePartitions),
	)
	require.NoError(t, err)

	// Verify min active partitions
	topicDesc, err = scope.Driver().Topic().Describe(ctx, topicPath)
	require.NoError(t, err)
	require.Equal(t, newMinActivePartitions, topicDesc.PartitionSettings.MinActivePartitions)
	require.Equal(t, 3, len(topicDesc.Partitions), "Expected 3 active partitions after increasing min active partitions")
}

func connect(t testing.TB, opts ...ydb.Option) *ydb.Driver {
	return connectWithLogOption(t, false, opts...)
}

func connectWithGrpcLogging(t testing.TB, opts ...ydb.Option) *ydb.Driver {
	return connectWithLogOption(t, true, opts...)
}

func connectWithLogOption(t testing.TB, logGRPC bool, opts ...ydb.Option) *ydb.Driver {
	connectionString := defaultConnectionString
	if cs := os.Getenv("YDB_CONNECTION_STRING"); cs != "" {
		connectionString = cs
	}

	var grpcOptions []grpc.DialOption
	const needLogGRPCMessages = true
	if logGRPC {
		grpcOptions = append(grpcOptions,
			grpc.WithChainUnaryInterceptor(xtest.NewGrpcLogger(t).UnaryClientInterceptor),
			grpc.WithChainStreamInterceptor(xtest.NewGrpcLogger(t).StreamClientInterceptor),
		)
	}

	ydbOpts := []ydb.Option{
		ydb.WithDialTimeout(time.Second),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		ydb.With(config.WithGrpcOptions(grpcOptions...)),
	}
	ydbOpts = append(ydbOpts, opts...)

	db, err := ydb.Open(context.Background(), connectionString, ydbOpts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close(context.Background())
	})
	return db
}
