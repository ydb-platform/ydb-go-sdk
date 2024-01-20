package topicwriterinternal

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestWriterWaitInit(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		mc := gomock.NewController(t)
		defer mc.Finish()

		strm := NewMockStreamWriter(mc)

		strm.EXPECT().WaitInit(ctx).Times(2)

		_, err := strm.WaitInit(ctx)
		require.NoError(t, err)

		// one more run is needed to check idempotency
		_, err = strm.WaitInit(ctx)
		require.NoError(t, err)
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
			require.NoError(t, w.Write(ctx, PublicMessage{SeqNo: 1}))
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
		require.NoError(t, w.Write(ctx, PublicMessage{SeqNo: 1}, PublicMessage{SeqNo: 3}))
	})
}
