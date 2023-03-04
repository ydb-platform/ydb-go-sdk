//go:build !fast
// +build !fast

package integration

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

const defaultConnectionString = "grpc://localhost:2136/local"

const commonConsumerName = "consumer"

func TestTopicCreateDrop(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	db := scope.Driver()
	topicPath := path.Join(scope.Folder(), "testtopic")

	err := db.Topic().Create(ctx, topicPath)
	scope.Require.NoError(err)

	_, err = db.Topic().Describe(ctx, topicPath)
	scope.Require.NoError(err)

	err = db.Topic().Drop(ctx, topicPath)
	scope.Require.NoError(err)

	_, err = db.Topic().Describe(ctx, topicPath)
	scope.Require.Error(err)
}

func TestTopicDescribe(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	db := scope.Driver()
	topicName := "topic"
	topicPath := path.Join(scope.Folder(), topicName)

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

	err := db.Topic().Create(ctx, topicPath,
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
	scope.Require.NoError(err)

	res, err := db.Topic().Describe(ctx, topicPath)
	scope.Require.NoError(err)

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
			scope.Require.True(ok, k)
			scope.Require.Equal(subValue, checkedValue)
		}
		*checked = nil
		*subset = nil
	}

	requireAndCleanSubset(&res.Attributes, &expected.Attributes)

	for i := range expected.Consumers {
		requireAndCleanSubset(&res.Consumers[i].Attributes, &expected.Consumers[i].Attributes)
	}

	scope.Require.Equal(expected, res)
}

func TestSchemeList(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	db := scope.Driver()
	topicPath := scope.TopicPath()
	list, err := db.Scheme().ListDirectory(ctx, scope.Folder())
	scope.Require.NoError(err)

	topicName := path.Base(topicPath)

	hasTopic := false
	for _, e := range list.Children {
		if e.IsTopic() && topicName == e.Name {
			hasTopic = true
		}
	}
	scope.Require.True(hasTopic)
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
