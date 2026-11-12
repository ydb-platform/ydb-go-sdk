//go:build go1.25

package topicwriterinternal

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jonboulle/clockwork"
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
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
				defer cancel()
				stop := make(chan struct{})
				stalled := make(chan struct{})
				cancelled := make(chan error, 3)
				sent := make(chan int64, 3)
				connectCount := 0
				ctrl := gomock.NewController(t)
				options := append(defaultTestWriterOptions(), WithAutoSetSeqNo(true), WithConnectFunc(
					func(streamCtx context.Context, _ *trace.Topic) (RawTopicWriterStream, error) {
						connectCount++
						attempt := connectCount
						if attempt >= 2 && attempt <= 4 && !stallInit {
							if attempt == 2 {
								close(stalled)
							}
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
								if attempt >= 2 && attempt <= 4 {
									if attempt == 2 {
										close(stalled)
									}
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
								if attempt >= 2 && attempt <= 4 {
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
				for range 3 {
					require.ErrorIs(t, <-cancelled, errConnTimeout)
				}
				require.EqualValues(t, 1, <-sent)
				require.EqualValues(t, 1, <-sent)
				time.Sleep(2 * time.Minute)
				require.NoError(t, writer.Write(ctx, newTestMessages(0)))
				require.NoError(t, writer.Flush(ctx))
				require.EqualValues(t, 2, <-sent)
				require.Equal(t, 5, connectCount)
			})
		})
	}
}

func TestWriterReconnector_RetriesInitResponseAfterConnectTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		stop := make(chan struct{})
		written := make(chan int, 1)
		ctrl := gomock.NewController(t)
		attempt := 0
		writer := newTestWriterStopped(
			WithConnectTimeout(time.Second),
			WithWaitAckOnWrite(true),
			WithConnectFunc(func(streamCtx context.Context, _ *trace.Topic) (RawTopicWriterStream, error) {
				attempt++
				streamAttempt := attempt
				initialized := false
				acks := make(chan rawtopicwriter.ServerMessage, 1)
				stream := NewMockRawTopicWriterStream(ctrl)
				stream.EXPECT().Endpoint().Return(nil).AnyTimes()
				stream.EXPECT().CloseSend().Return(nil).AnyTimes()
				stream.EXPECT().Send(gomock.Any()).DoAndReturn(func(msg rawtopicwriter.ClientMessage) error {
					if streamCtx.Err() != nil {
						return xerrors.Transport(status.Error(codes.Canceled, "stream cancelled"))
					}
					if request, ok := msg.(*rawtopicwriter.WriteRequest); ok {
						written <- streamAttempt
						acks <- &rawtopicwriter.WriteResult{Acks: []rawtopicwriter.WriteAck{
							{SeqNo: request.Messages[0].SeqNo},
						}}
					}

					return nil
				}).AnyTimes()
				stream.EXPECT().Recv().DoAndReturn(func() (rawtopicwriter.ServerMessage, error) {
					if !initialized {
						initialized = true
						if streamAttempt == 1 {
							// A buffered response may arrive after the connection timer fires.
							<-streamCtx.Done()
						}

						return &rawtopicwriter.InitResult{
							SupportedCodecs: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw},
						}, nil
					}
					select {
					case ack := <-acks:
						return ack, nil
					case <-streamCtx.Done():
						return nil, xerrors.Transport(status.Error(codes.Canceled, "stream cancelled"))
					case <-stop:
						return nil, io.EOF
					}
				}).AnyTimes()

				return stream, nil
			}),
		)
		defer func() {
			close(stop)
			_ = writer.close(context.Background(), io.EOF)
		}()
		writer.start()

		require.NoError(t, writer.Write(ctx, newTestMessages(1)))
		require.Equal(t, 2, <-written)
	})
}

func TestWriterReconnector_CloseDuringInit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		cancelled := make(chan error, 1)
		ctrl := gomock.NewController(t)
		writer := newTestWriterStopped(
			WithConnectTimeout(time.Minute),
			WithConnectFunc(func(streamCtx context.Context, _ *trace.Topic) (RawTopicWriterStream, error) {
				stream := NewMockRawTopicWriterStream(ctrl)
				stream.EXPECT().Endpoint().Return(nil).AnyTimes()
				stream.EXPECT().CloseSend().Return(nil).AnyTimes()
				stream.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
				stream.EXPECT().Recv().DoAndReturn(func() (rawtopicwriter.ServerMessage, error) {
					close(entered)
					<-streamCtx.Done()
					cancelled <- context.Cause(streamCtx)

					return nil, xerrors.Transport(status.Error(codes.Canceled, "stream cancelled"))
				})

				return stream, nil
			}),
		)
		defer func() {
			_ = writer.close(context.Background(), io.EOF)
		}()
		writer.start()
		<-entered

		started := time.Now()
		require.NoError(t, writer.Close(t.Context()))
		require.ErrorIs(t, <-cancelled, context.Canceled, "Close returned after %s", time.Since(started))
		require.Zero(t, time.Since(started), "Close must cancel init without waiting for its timeout")
	})
}

func TestWriterReconnector_InitTimeoutPreservesRetryBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		clock := clockwork.NewFakeClock()
		entered := make(chan int, 2)
		ctrl := gomock.NewController(t)
		attempt := 0
		writer := newTestWriterStopped(
			WithClock(clock),
			WithConnectTimeout(100*time.Millisecond),
			WithStartTimeout(time.Second),
			WithConnectFunc(func(streamCtx context.Context, _ *trace.Topic) (RawTopicWriterStream, error) {
				attempt++
				stream := NewMockRawTopicWriterStream(ctrl)
				stream.EXPECT().Endpoint().Return(nil).AnyTimes()
				stream.EXPECT().CloseSend().Return(nil).AnyTimes()
				stream.EXPECT().Send(gomock.Any()).Return(nil)
				stream.EXPECT().Recv().DoAndReturn(func() (rawtopicwriter.ServerMessage, error) {
					entered <- attempt
					<-streamCtx.Done()

					return nil, xerrors.Transport(status.Error(codes.Canceled, "stream cancelled"))
				})

				return stream, nil
			}),
		)
		defer func() {
			_ = writer.close(context.Background(), io.EOF)
		}()
		writer.start()
		require.Equal(t, 1, <-entered)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		require.NoError(t, clock.BlockUntilContext(ctx, 1))

		// A delayed retry is not evidence that an unsuccessful connection became stable.
		time.Sleep(2 * time.Second)
		clock.Advance(2 * time.Second)
		require.Equal(t, 2, <-entered)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		require.True(t, isClosed(writer.background.Done()), "failed init must not reset the exhausted retry budget")
		require.ErrorContains(t, writer.WaitInit(ctx), "reconnection timeout")
		require.ErrorIs(t, writer.Write(ctx, newTestMessages(1)), ErrPublicWriterClosed)
		require.Equal(t, 2, attempt)
	})
}
