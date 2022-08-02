package topicwriterinternal

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
)

func BenchmarkWriteMessageAllocations(b *testing.B) {
	b.ReportAllocs()
	w := &Writer{
		streamWriter: writerReturnToPool{},
	}
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		_ = w.Write(ctx, Message{SeqNo: int64(i)})
	}
}

func TestWriterWrite(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		ctx := context.Background()
		t.Run("OK", func(t *testing.T) {
			mc := gomock.NewController(t)

			strm := NewMockstreamWriter(mc)
			strm.EXPECT().Write(ctx, newTestMessages(1))
			w := Writer{
				streamWriter: strm,
			}
			require.NoError(t, w.Write(ctx, Message{SeqNo: 1}))
		})
	})
}

func TestWriterWriteMessage(t *testing.T) {
	ctx := context.Background()
	t.Run("OK", func(t *testing.T) {
		mc := gomock.NewController(t)

		strm := NewMockstreamWriter(mc)
		strm.EXPECT().Write(ctx, newTestMessages(1, 3))
		w := Writer{
			streamWriter: strm,
		}
		require.NoError(t, w.Write(ctx, Message{SeqNo: 1}, Message{SeqNo: 3}))
	})
}

type writerReturnToPool struct{}

func (w writerReturnToPool) Write(ctx context.Context, messages *messageWithDataContentSlice) (rawtopicwriter.WriteResult, error) {
	for i := 0; i < len(messages.m); i++ {
		putBuffer(messages.m[i].buf)
	}
	putContentMessagesSlice(messages)
	return rawtopicwriter.WriteResult{}, nil
}

func (w writerReturnToPool) Close(ctx context.Context) error {
	return nil
}
