package topicwriterinternal

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestWriterWaitInit(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		mc := gomock.NewController(t)
		defer mc.Finish()

		strm := NewMockStreamWriter(mc)

		callsCount := 5
		strm.EXPECT().WaitInit(ctx).Times(5)

		for i := 1; i <= callsCount; i++ {
			_, err := strm.WaitInit(ctx)
			require.NoError(t, err)
		}

	})
}

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
