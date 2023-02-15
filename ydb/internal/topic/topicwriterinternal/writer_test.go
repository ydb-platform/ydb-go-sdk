package topicwriterinternal

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestWriterWrite(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		ctx := context.Background()
		t.Run("OK", func(t *testing.T) {
			mc := gomock.NewController(t)

			strm := NewMockStreamWriter(mc)
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

		strm := NewMockStreamWriter(mc)
		strm.EXPECT().Write(ctx, newTestMessages(1, 3))
		w := Writer{
			streamWriter: strm,
		}
		require.NoError(t, w.Write(ctx, Message{SeqNo: 1}, Message{SeqNo: 3}))
	})
}
