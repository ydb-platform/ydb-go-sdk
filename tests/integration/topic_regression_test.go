//go:build integration
// +build integration

package integration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestRegressionIssue1011_WriteInitInfoLastSeqNum(t *testing.T) {
	scope := newScope(t)
	w1 := scope.TopicWriter()
	err := w1.Write(scope.Ctx, topicwriter.Message{
		Data: strings.NewReader("123"),
	})
	require.NoError(t, err)
	require.NoError(t, w1.Close(scope.Ctx))

	// Check
	w2, err := scope.Driver().Topic().StartWriter(
		scope.TopicPath(),
		topicoptions.WithWriterProducerID(scope.TopicWriterProducerID()),
		topicoptions.WithWriterSetAutoSeqNo(false),
	)
	require.NoError(t, err)

	info, err := w2.WaitInitInfo(scope.Ctx)
	require.Equal(t, int64(1), info.LastSeqNum)
}
