//go:build go1.25

package topicwriterinternal

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestWriterReconnector_DefaultConnectTimeout(t *testing.T) {
	for _, stallInit := range []bool{false, true} {
		name := "Connect"
		if stallInit {
			name = "Init"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
				defer cancel()
				stop := make(chan struct{})
				stalled := make(chan struct{})
				cancelled := make(chan error, 1)
				sent := make(chan int64, 3)
				connectCount := 0
				ctrl := gomock.NewController(t)
				options := append(defaultTestWriterOptions(), WithAutoSetSeqNo(true), WithConnectFunc(
					func(streamCtx context.Context, _ *trace.Topic) (RawTopicWriterStream, error) {
						connectCount++
						attempt := connectCount
						if attempt == 2 && !stallInit {
							close(stalled)
							select {
							case <-streamCtx.Done():
								cancelled <- context.Cause(streamCtx)

								return nil, xerrors.Transport(status.Error(codes.Canceled, "connection cancelled"))
							case <-stop:
							}

							return nil, io.EOF
						}
						stream := NewMockRawTopicWriterStream(ctrl)
						responses := make(chan sendFromServerResponse, 1)
						stream.EXPECT().Endpoint().Return(nil).AnyTimes()
						stream.EXPECT().CloseSend().Return(nil).AnyTimes()
						stream.EXPECT().Send(gomock.Any()).DoAndReturn(func(msg rawtopicwriter.ClientMessage) error {
							switch msg := msg.(type) {
							case *rawtopicwriter.InitRequest:
								if attempt == 2 {
									close(stalled)
								} else {
									responses <- sendFromServerResponse{msg: &rawtopicwriter.InitResult{
										LastSeqNo:       0,
										SupportedCodecs: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw},
									}}
								}
							case *rawtopicwriter.WriteRequest:
								acks := make([]rawtopicwriter.WriteAck, len(msg.Messages))
								for i, message := range msg.Messages {
									sent <- message.SeqNo
									acks[i] = rawtopicwriter.WriteAck{SeqNo: message.SeqNo}
								}
								if attempt == 1 {
									responses <- sendFromServerResponse{err: io.EOF}
								} else {
									responses <- sendFromServerResponse{msg: &rawtopicwriter.WriteResult{Acks: acks}}
								}
							}

							return nil
						}).AnyTimes()
						stream.EXPECT().Recv().DoAndReturn(func() (rawtopicwriter.ServerMessage, error) {
							select {
							case response := <-responses:
								return response.msg, response.err
							case <-streamCtx.Done():
								if attempt == 2 {
									cancelled <- context.Cause(streamCtx)
								}

								return nil, xerrors.Transport(status.Error(codes.Canceled, "stream cancelled"))
							case <-stop:
								return nil, io.EOF
							}
						}).AnyTimes()

						return stream, nil
					}))
				writer := newWriterReconnectorStopped(NewWriterReconnectorConfig(options...))
				defer func() {
					close(stop)
					_ = writer.close(context.Background(), io.EOF)
				}()
				writer.start()
				require.NoError(t, writer.waitFirstInitResponse(ctx))
				require.NoError(t, writer.Write(ctx, newTestMessages(0)))
				select {
				case <-stalled:
				case <-ctx.Done():
					t.Fatal("reconnection did not start")
				}
				require.NoError(t, writer.Flush(ctx))
				require.ErrorIs(t, <-cancelled, errConnTimeout)
				require.EqualValues(t, 1, <-sent)
				require.EqualValues(t, 1, <-sent)
				time.Sleep(2 * time.Minute)
				require.NoError(t, writer.Write(ctx, newTestMessages(0)))
				require.NoError(t, writer.Flush(ctx))
				require.EqualValues(t, 2, <-sent)
				require.Equal(t, 3, connectCount)
			})
		})
	}
}
