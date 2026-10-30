package topicmultiwriter

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestMultiWriter_CustomEncoder(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	const codec = rawtopiccommon.Codec(10001)
	multiWriter := newTestMultiWriterWithBasicWriter(t, stubClient.Describe,
		topicwriterinternal.WithAddEncoder(codec, func(output io.Writer) (io.WriteCloser, error) {
			return gzip.NewWriter(output), nil
		}),
	)
	t.Cleanup(func() { require.NoError(t, multiWriter.Close(ctx)) })
	require.NoError(t, multiWriter.WaitInit(ctx))

	const payload = "custom codec payload"
	var output bytes.Buffer
	n, err := multiWriter.encoders.EncodeBytes(codec, &output, []byte(payload))
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	decoder, err := gzip.NewReader(&output)
	require.NoError(t, err)
	defer func() { require.NoError(t, decoder.Close()) }()
	decoded, err := io.ReadAll(decoder)
	require.NoError(t, err)
	require.Equal(t, payload, string(decoded))
}
