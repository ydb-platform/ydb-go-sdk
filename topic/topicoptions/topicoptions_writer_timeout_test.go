//go:build go1.25

package topicoptions

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestWithWriterConnectTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var common config.Common
		config.SetOperationTimeout(&common, time.Minute)
		streamCtx := stalledWriterConnect(t,
			topicwriterinternal.WithCommonConfig(common),
			WithWriterConnectTimeout(time.Second),
		)

		time.Sleep(2 * time.Second)
		synctest.Wait()

		require.Error(t, streamCtx.Err(), "the writer must cancel the connection attempt after its timeout")
	})
}

func TestWithWriterConnectTimeoutDisablesLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var common config.Common
		config.SetOperationTimeout(&common, time.Second)
		streamCtx := stalledWriterConnect(t,
			topicwriterinternal.WithCommonConfig(common),
			WithWriterConnectTimeout(-time.Second),
		)

		time.Sleep(2 * time.Minute)
		synctest.Wait()

		require.NoError(t, streamCtx.Err(), "a negative timeout must disable the connection limit")
	})
}

func stalledWriterConnect(t *testing.T, options ...WriterOption) context.Context {
	t.Helper()
	started := make(chan context.Context, 1)
	stop := make(chan struct{})
	options = append(options, topicwriterinternal.WithConnectFunc(
		func(ctx context.Context, _ *trace.Topic) (topicwriterinternal.RawTopicWriterStream, error) {
			select {
			case started <- ctx:
			default:
			}
			select {
			case <-ctx.Done():
			case <-stop:
			}

			return nil, xerrors.Transport(status.Error(codes.Canceled, "connection cancelled"))
		},
	))
	writer, err := topicwriterinternal.NewWriterReconnector(topicwriterinternal.NewWriterReconnectorConfig(options...))
	require.NoError(t, err)
	t.Cleanup(func() {
		close(stop)
		require.NoError(t, writer.Close(context.Background()))
	})

	return <-started
}
