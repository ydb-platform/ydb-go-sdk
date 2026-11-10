package topicmultiwriter

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestMultiWriterReusedBoundChooserIsolatesClusterTopologies(t *testing.T) {
	chooser := BoundPartitionChooser(partitionchooser.WithKeyHasher(func(key string) string { return key }))
	option := PublicMultiWriterOption(WithWriterPartitionByKey(chooser))
	factoryA := &lastSeqWritersFactory{writers: map[int64]*orderedSeqWriter{
		0: {writes: make(chan int64, 2)},
	}}
	a, ctx := newMultiWriterForSplitRace(t, factoryA, option)
	factoryB := &lastSeqWritersFactory{writers: map[int64]*orderedSeqWriter{
		2: {writes: make(chan int64, 1)},
	}}
	b, _ := newMultiWriterForSplitRaceWithDescriber(t, factoryB, func(context.Context, string) (
		topictypes.TopicDescription, error,
	) {
		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 2, Active: true, ToBound: []byte("g")},
			{PartitionID: 3, Active: true, FromBound: []byte("g")},
		}}, nil
	}, option)
	for _, w := range []*MultiWriter{a, b, a} {
		require.NoError(t, w.Write(ctx, []topicwriterinternal.PublicMessage{{
			Key: "a", Data: bytes.NewReader([]byte("message")),
		}}))
		require.NoError(t, w.Flush(ctx))
	}
	require.Equal(t, int64(1), <-factoryA.writers[0].writes)
	require.Equal(t, int64(2), <-factoryA.writers[0].writes)
	require.Equal(t, int64(1), <-factoryB.writers[2].writes)
	require.NoError(t, a.Close(ctx))
	require.NoError(t, b.Close(ctx))
}

func TestMultiWriterReusedHashChooserIsolatesClusterTopologies(t *testing.T) {
	option := PublicMultiWriterOption(WithWriterPartitionByKey(KafkaHashPartitionChooser()))
	newWriter := func(ids ...int64) *MultiWriter {
		w, _ := newMultiWriterForSplitRaceWithDescriber(t,
			&lastSeqWritersFactory{writers: make(map[int64]*orderedSeqWriter)},
			func(context.Context, string) (topictypes.TopicDescription, error) {
				var partitions []topictypes.PartitionInfo
				for _, id := range ids {
					partitions = append(partitions, topictypes.PartitionInfo{PartitionID: id, Active: true})
				}

				return topictypes.TopicDescription{Partitions: partitions}, nil
			}, option)

		return w
	}
	a := newWriter(0, 1)
	b := newWriter(10, 11)
	for _, key := range []string{"a", "b", "c", "d", "key0", "key1"} {
		for _, w := range []*MultiWriter{a, b, a} {
			msg := message{MessageWithDataContent: topicwritercommon.NewMessageDataWithContent(
				topicwriterinternal.PublicMessage{Key: key}, nil)}
			id, err := w.orchestrator.choosePartition(msg)
			require.NoError(t, err)
			if w == a {
				require.Contains(t, []int64{0, 1}, id, "writer A must keep its own topology")
			} else {
				require.Contains(t, []int64{10, 11}, id, "writer B must keep its own topology")
			}
		}
	}
}
