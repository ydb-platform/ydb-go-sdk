//go:build integration
// +build integration

package integration

import (
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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
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

	res, err := db.Topic().Describe(ctx, topicName)
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

	requireAndCleanSubset(&res.Attributes, &expected.Attributes)

	for i := range expected.Consumers {
		requireAndCleanSubset(&res.Consumers[i].Attributes, &expected.Consumers[i].Attributes)
	}

	require.Equal(t, expected, res)
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
		if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
			t.Skip("Read topic without consumer implemented since YDB 24.1, test ran for '" + os.Getenv("YDB_VERSION") + "'")
		}
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
