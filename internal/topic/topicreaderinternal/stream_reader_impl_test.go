package topicreaderinternal

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicStreamReaderImpl_BufferCounterOnStopPartition(t *testing.T) {
	table := []struct {
		name     string
		graceful bool
	}{
		{
			name:     "graceful",
			graceful: true,
		},
		{
			name:     "force",
			graceful: false,
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			e := newTopicReaderTestEnv(t)
			e.Start()

			initialBufferSize := e.reader.restBufferSizeBytes.Load()
			messageSize := initialBufferSize - 1

			e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: int(messageSize)}).MaxTimes(1)

			messageReaded := make(empty.Chan)
			e.SendFromServerAndSetNextCallback(&rawtopicreader.ReadResponse{
				BytesSize: int(messageSize),
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID,
						Batches: []rawtopicreader.Batch{
							{
								Codec:            0,
								ProducerID:       "",
								WriteSessionMeta: nil,
								WrittenAt:        time.Time{},
								MessageData: []rawtopicreader.MessageData{
									{
										Offset: 1,
										SeqNo:  1,
									},
								},
							},
						},
					},
				},
			}, func() {
				close(messageReaded)
			})
			<-messageReaded
			require.Equal(t, int64(1), e.reader.restBufferSizeBytes.Load())

			partitionStopped := make(empty.Chan)
			e.SendFromServerAndSetNextCallback(&rawtopicreader.StopPartitionSessionRequest{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{},
				PartitionSessionID:    e.partitionSessionID,
				Graceful:              test.graceful,
				CommittedOffset:       0,
			}, func() {
				close(partitionStopped)
			})
			<-partitionStopped

			fixedBufferSizeCtx, cancel := context.WithCancel(e.ctx)
			go func() {
				xtest.SpinWaitCondition(t, nil, func() bool {
					return initialBufferSize == e.reader.restBufferSizeBytes.Load()
				})
				cancel()
			}()

			_, _ = e.reader.ReadMessageBatch(fixedBufferSizeCtx, newReadMessageBatchOptions())
			<-fixedBufferSizeCtx.Done()
			require.Equal(t, initialBufferSize, e.reader.restBufferSizeBytes.Load())
		})
	}
}

func TestTopicStreamReaderImpl_CommitStolen(t *testing.T) {
	xtest.TestManyTimesWithName(t, "SimpleCommit", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		lastOffset := e.partitionSession.LastReceivedMessageOffset()
		const dataSize = 4

		// request new data portion
		readRequestReceived := make(empty.Chan)
		e.stream.EXPECT().Send(
			&rawtopicreader.ReadRequest{BytesSize: dataSize * 2},
		).DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
			close(readRequestReceived)

			return nil
		})

		commitReceived := make(empty.Chan)
		// Expect commit message with stole
		e.stream.EXPECT().Send(
			&rawtopicreader.CommitOffsetRequest{
				CommitOffsets: []rawtopicreader.PartitionCommitOffset{
					{
						PartitionSessionID: e.partitionSessionID,
						Offsets: []rawtopiccommon.OffsetRange{
							{
								Start: lastOffset + 1,
								End:   lastOffset + 16,
							},
						},
					},
				},
			},
		).DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
			close(commitReceived)

			return nil
		})

		// send message with stole offsets
		//
		e.SendFromServer(&rawtopicreader.ReadResponse{
			BytesSize: dataSize,
			PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							Codec:      rawtopiccommon.CodecRaw,
							ProducerID: "1",
							MessageData: []rawtopicreader.MessageData{
								{
									Offset: lastOffset + 10,
								},
							},
						},
					},
				},
			},
		})

		e.SendFromServer(&rawtopicreader.ReadResponse{
			BytesSize: dataSize,
			PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							Codec:      rawtopiccommon.CodecRaw,
							ProducerID: "1",
							MessageData: []rawtopicreader.MessageData{
								{
									Offset: lastOffset + 15,
								},
							},
						},
					},
				},
			},
		})

		opts := newReadMessageBatchOptions()
		opts.MinCount = 2
		batch, err := e.reader.ReadMessageBatch(e.ctx, opts)
		require.NoError(t, err)
		require.NoError(t, e.reader.Commit(e.ctx, topicreadercommon.GetCommitRange(batch)))
		xtest.WaitChannelClosed(t, commitReceived)
		xtest.WaitChannelClosed(t, readRequestReceived)
	})
	xtest.TestManyTimesWithName(t, "WrongOrderCommitWithSyncMode", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.reader.cfg.CommitMode = topicreadercommon.CommitModeSync
		e.Start()

		lastOffset := e.partitionSession.LastReceivedMessageOffset()
		const dataSize = 4
		// request new data portion
		readRequestReceived := make(empty.Chan)
		e.stream.EXPECT().Send(
			&rawtopicreader.ReadRequest{BytesSize: dataSize * 2},
		).DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
			close(readRequestReceived)

			return nil
		})

		e.SendFromServer(&rawtopicreader.ReadResponse{
			BytesSize: dataSize,
			PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							Codec:      rawtopiccommon.CodecRaw,
							ProducerID: "1",
							MessageData: []rawtopicreader.MessageData{
								{
									Offset: lastOffset + 1,
								},
							},
						},
					},
				},
			},
		})

		e.SendFromServer(&rawtopicreader.ReadResponse{
			BytesSize: dataSize,
			PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							Codec:      rawtopiccommon.CodecRaw,
							ProducerID: "1",
							MessageData: []rawtopicreader.MessageData{
								{
									Offset: lastOffset + 2,
								},
							},
						},
					},
				},
			},
		})

		opts := newReadMessageBatchOptions()
		opts.MinCount = 2
		batch, err := e.reader.ReadMessageBatch(e.ctx, opts)
		require.NoError(t, err)
		require.ErrorIs(t, e.reader.Commit(
			e.ctx,
			topicreadercommon.GetCommitRange(batch.Messages[1]),
		), topicreadercommon.ErrWrongCommitOrderInSyncMode)
		xtest.WaitChannelClosed(t, readRequestReceived)
	})

	xtest.TestManyTimesWithName(t, "CommitAfterGracefulStopPartition", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)

		committed := e.partitionSession.CommittedOffset()
		commitReceived := make(empty.Chan)
		e.stream.EXPECT().Send(&rawtopicreader.CommitOffsetRequest{CommitOffsets: []rawtopicreader.PartitionCommitOffset{
			{
				PartitionSessionID: e.partitionSessionID,
				Offsets: []rawtopiccommon.OffsetRange{
					{
						Start: committed,
						End:   committed + 1,
					},
				},
			},
		}}).DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
			close(commitReceived)

			return nil
		})

		stopPartitionResponseSent := make(empty.Chan)
		e.stream.EXPECT().Send(&rawtopicreader.StopPartitionSessionResponse{PartitionSessionID: e.partitionSessionID}).
			DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
				close(stopPartitionResponseSent)

				return nil
			})

		e.Start()

		// send from server message, then partition graceful stop request
		go func() {
			e.SendFromServer(&rawtopicreader.ReadResponse{
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID,
						Batches: []rawtopicreader.Batch{
							{
								Codec: rawtopiccommon.CodecRaw,
								MessageData: []rawtopicreader.MessageData{
									{
										Offset: committed,
										SeqNo:  1,
									},
								},
							},
						},
					},
				},
			})
			e.SendFromServer(&rawtopicreader.StopPartitionSessionRequest{
				PartitionSessionID: e.partitionSessionID,
				Graceful:           true,
			})
		}()

		readCtx, readCtxCancel := xcontext.WithCancel(e.ctx)
		go func() {
			<-stopPartitionResponseSent
			readCtxCancel()
		}()

		batch, err := e.reader.ReadMessageBatch(readCtx, newReadMessageBatchOptions())
		require.NoError(t, err)
		err = e.reader.Commit(e.ctx, topicreadercommon.GetCommitRange(batch))
		require.NoError(t, err)
		_, err = e.reader.ReadMessageBatch(readCtx, newReadMessageBatchOptions())
		require.ErrorIs(t, err, context.Canceled)

		select {
		case <-e.partitionSession.Context().Done():
			// pass
		case <-time.After(time.Second):
			t.Fatal("partition session not closed")
		}

		xtest.WaitChannelClosed(t, commitReceived)
	})
}

func TestTopicStreamReaderImpl_Create(t *testing.T) {
	xtest.TestManyTimesWithName(t, "BadSessionInitialization", func(t testing.TB) {
		mc := gomock.NewController(t)
		stream := NewMockRawTopicReaderStream(mc)
		stream.EXPECT().Send(gomock.Any()).Return(nil)
		stream.EXPECT().Recv().Return(&rawtopicreader.StartPartitionSessionRequest{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusInternalError},
		}, nil)
		stream.EXPECT().CloseSend().Return(nil)

		reader, err := newTopicStreamReader(nil, topicreadercommon.NextReaderID(), stream, newTopicStreamReaderConfig())
		require.Error(t, err)
		require.Nil(t, reader)
	})
}

func TestTopicStreamReaderImpl_WaitInit(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)
		e.Start()
		err := e.reader.WaitInit(context.Background())
		require.NoError(t, err)
	})

	t.Run("not started", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)
		err := e.reader.WaitInit(context.Background())
		require.Error(t, err)
	})
}

func TestStreamReaderImpl_OnPartitionCloseHandle(t *testing.T) {
	xtest.TestManyTimesWithName(t, "GracefulFalseCancelPartitionContext", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		require.NoError(t, e.partitionSession.Context().Err())

		// stop partition
		e.SendFromServerAndSetNextCallback(
			&rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: e.partitionSessionID},
			func() {
				require.Error(t, e.partitionSession.Context().Err())
			})
		e.WaitMessageReceived()
	})
	xtest.TestManyTimesWithName(t, "TraceGracefulTrue", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)

		readMessagesCtx, readMessagesCtxCancel := xcontext.WithCancel(context.Background())
		committedOffset := int64(222)

		e.reader.cfg.Trace.OnReaderPartitionReadStopResponse = func(info trace.TopicReaderPartitionReadStopResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) { //nolint:lll
			expected := trace.TopicReaderPartitionReadStopResponseStartInfo{
				ReaderConnectionID: e.reader.readConnectionID,
				PartitionContext:   e.partitionSession.Context(),
				Topic:              e.partitionSession.Topic,
				PartitionID:        e.partitionSession.PartitionID,
				PartitionSessionID: e.partitionSession.StreamPartitionSessionID.ToInt64(),
				CommittedOffset:    committedOffset,
				Graceful:           true,
			}
			require.Equal(t, expected, info)

			require.NoError(t, info.PartitionContext.Err())

			readMessagesCtxCancel()

			return nil
		}

		e.Start()

		stopPartitionResponseSent := make(empty.Chan)
		e.stream.EXPECT().Send(&rawtopicreader.StopPartitionSessionResponse{
			PartitionSessionID: e.partitionSessionID,
		}).DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
			close(stopPartitionResponseSent)

			return nil
		})

		e.SendFromServer(&rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: e.partitionSessionID,
			Graceful:           true,
			CommittedOffset:    rawtopiccommon.NewOffset(committedOffset),
		})

		_, err := e.reader.ReadMessageBatch(readMessagesCtx, newReadMessageBatchOptions())
		require.Error(t, err)
		require.Error(t, readMessagesCtx.Err())
		xtest.WaitChannelClosed(t, stopPartitionResponseSent)
	})
	xtest.TestManyTimesWithName(t, "TraceGracefulFalse", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)

		readMessagesCtx, readMessagesCtxCancel := xcontext.WithCancel(context.Background())
		committedOffset := int64(222)

		e.reader.cfg.Trace.OnReaderPartitionReadStopResponse = func(info trace.TopicReaderPartitionReadStopResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) { //nolint:lll
			expected := trace.TopicReaderPartitionReadStopResponseStartInfo{
				ReaderConnectionID: e.reader.readConnectionID,
				PartitionContext:   e.partitionSession.Context(),
				Topic:              e.partitionSession.Topic,
				PartitionID:        e.partitionSession.PartitionID,
				PartitionSessionID: e.partitionSession.StreamPartitionSessionID.ToInt64(),
				CommittedOffset:    committedOffset,
				Graceful:           false,
			}
			require.Equal(t, expected, info)
			require.Error(t, info.PartitionContext.Err())

			readMessagesCtxCancel()

			return nil
		}

		e.Start()

		e.SendFromServer(&rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: e.partitionSessionID,
			Graceful:           false,
			CommittedOffset:    rawtopiccommon.NewOffset(committedOffset),
		})

		_, err := e.reader.ReadMessageBatch(readMessagesCtx, newReadMessageBatchOptions())
		require.Error(t, err)
		require.Error(t, readMessagesCtx.Err())
	})
}

func TestStreamReaderImpl_TestEndSessionForSplitMergePartitions(t *testing.T) {
	t.Run("Split", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			e := newTopicReaderTestEnv(t)

			// doesn't check sends
			e.stream.EXPECT().Send(gomock.Any()).Return(nil).MinTimes(0)

			e.Start()

			activePartitionID := e.partitionSession.PartitionID

			e.SendFromServer(&rawtopicreader.ReadResponse{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID,
						Batches: []rawtopicreader.Batch{{
							Codec:       rawtopiccommon.CodecRaw,
							MessageData: []rawtopicreader.MessageData{{Offset: 1, Data: []byte("OK")}},
						}},
					},
				},
			})

			e.SendFromServer(&rawtopicreader.EndPartitionSession{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionSessionID:    e.partitionSessionID,
				ChildPartitionIDs:     []int64{activePartitionID + 1, activePartitionID + 2},
			})

			e.SendFromServer(&rawtopicreader.StartPartitionSessionRequest{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionSession: rawtopicreader.PartitionSession{
					PartitionSessionID: e.partitionSessionID + 1,
					Path:               e.partitionSession.Topic,
					PartitionID:        activePartitionID + 1,
				},
			})

			e.SendFromServer(&rawtopicreader.StartPartitionSessionRequest{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionSession: rawtopicreader.PartitionSession{
					PartitionSessionID: e.partitionSessionID + 2,
					Path:               e.partitionSession.Topic,
					PartitionID:        activePartitionID + 2,
				},
			})

			e.SendFromServer(&rawtopicreader.ReadResponse{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID + 1,
						Batches: []rawtopicreader.Batch{{
							Codec:       rawtopiccommon.CodecRaw,
							MessageData: []rawtopicreader.MessageData{{Offset: 1, Data: []byte("BAD-1")}},
						}},
					},
				},
			})

			allMessagesSent := make(empty.Chan)
			e.SendFromServerAndSetNextCallback(&rawtopicreader.ReadResponse{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID + 2,
						Batches: []rawtopicreader.Batch{{
							Codec:       rawtopiccommon.CodecRaw,
							MessageData: []rawtopicreader.MessageData{{Offset: 1, Data: []byte("BAD-2")}},
						}},
					},
				},
			}, func() {
				close(allMessagesSent)
			})

			<-allMessagesSent
			batch, err := e.reader.ReadMessageBatch(e.ctx, newReadMessageBatchOptions())
			require.NoError(t, err)
			require.Same(t, e.partitionSession, topicreadercommon.BatchGetPartitionSession(batch))
		})
	})
	t.Run("Merge", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			e := newTopicReaderTestEnv(t)

			// doesn't check sends
			e.stream.EXPECT().Send(gomock.Any()).Return(nil).MinTimes(0)

			e.Start()

			activePartitionID := e.partitionSession.PartitionID

			e.SendFromServer(&rawtopicreader.ReadResponse{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID,
						Batches: []rawtopicreader.Batch{{
							Codec:       rawtopiccommon.CodecRaw,
							MessageData: []rawtopicreader.MessageData{{Offset: 1, Data: []byte("OK")}},
						}},
					},
				},
			})

			e.SendFromServer(&rawtopicreader.EndPartitionSession{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionSessionID:    e.partitionSessionID,
				ChildPartitionIDs:     []int64{activePartitionID + 1, activePartitionID + 2},
			})

			e.SendFromServer(&rawtopicreader.StartPartitionSessionRequest{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionSession: rawtopicreader.PartitionSession{
					PartitionSessionID: e.partitionSessionID + 1,
					Path:               e.partitionSession.Topic,
					PartitionID:        activePartitionID + 1,
				},
			})

			e.SendFromServer(&rawtopicreader.StartPartitionSessionRequest{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionSession: rawtopicreader.PartitionSession{
					PartitionSessionID: e.partitionSessionID + 2,
					Path:               e.partitionSession.Topic,
					PartitionID:        activePartitionID + 2,
				},
			})

			e.SendFromServer(&rawtopicreader.ReadResponse{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID + 1,
						Batches: []rawtopicreader.Batch{{
							Codec:       rawtopiccommon.CodecRaw,
							MessageData: []rawtopicreader.MessageData{{Offset: 1, Data: []byte("BAD-1")}},
						}},
					},
				},
			})

			allMessagesSent := make(empty.Chan)
			e.SendFromServerAndSetNextCallback(&rawtopicreader.ReadResponse{
				ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID + 2,
						Batches: []rawtopicreader.Batch{{
							Codec:       rawtopiccommon.CodecRaw,
							MessageData: []rawtopicreader.MessageData{{Offset: 1, Data: []byte("BAD-2")}},
						}},
					},
				},
			}, func() {
				close(allMessagesSent)
			})

			<-allMessagesSent
			batch, err := e.reader.ReadMessageBatch(e.ctx, newReadMessageBatchOptions())
			require.NoError(t, err)
			require.Same(t, e.partitionSession, topicreadercommon.BatchGetPartitionSession(batch))
		})
	})
}

func TestTopicStreamReaderImpl_ReadMessages(t *testing.T) {
	t.Run("BufferSize", func(t *testing.T) {
		waitChangeRestBufferSizeBytes := func(r *topicStreamReaderImpl, old int64) {
			xtest.SpinWaitCondition(t, nil, func() bool {
				return r.restBufferSizeBytes.Load() != old
			})
		}

		xtest.TestManyTimesWithName(t, "InitialBufferSize", func(t testing.TB) {
			e := newTopicReaderTestEnv(t)
			e.Start()
			waitChangeRestBufferSizeBytes(e.reader, 0)
			require.Equal(t, e.initialBufferSizeBytes, e.reader.restBufferSizeBytes.Load())
		})

		xtest.TestManyTimesWithName(t, "DecrementIncrementBufferSize", func(t testing.TB) {
			e := newTopicReaderTestEnv(t)

			// doesn't check sends
			e.stream.EXPECT().Send(gomock.Any()).Return(nil).MinTimes(1)

			e.Start()
			waitChangeRestBufferSizeBytes(e.reader, 0)

			const dataSize = 1000
			e.SendFromServer(&rawtopicreader.ReadResponse{BytesSize: dataSize, PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							MessageData: []rawtopicreader.MessageData{
								{
									Offset: 1,
									SeqNo:  1,
									Data:   []byte{1, 2},
								},
								{
									Offset: 2,
									SeqNo:  2,
									Data:   []byte{4, 5, 6},
								},
								{
									Offset: 3,
									SeqNo:  3,
									Data:   []byte{7},
								},
							},
						},
					},
				},
			}})
			waitChangeRestBufferSizeBytes(e.reader, e.initialBufferSizeBytes)
			expectedBufferSizeAfterReceiveMessages := e.initialBufferSizeBytes - dataSize
			require.Equal(t, expectedBufferSizeAfterReceiveMessages, e.reader.restBufferSizeBytes.Load())

			oneOption := newReadMessageBatchOptions()
			oneOption.MaxCount = 1
			_, err := e.reader.ReadMessageBatch(e.ctx, oneOption)
			require.NoError(t, err)

			waitChangeRestBufferSizeBytes(e.reader, expectedBufferSizeAfterReceiveMessages)

			bufferSizeAfterReadOneMessage := e.reader.restBufferSizeBytes.Load()

			_, err = e.reader.ReadMessageBatch(e.ctx, newReadMessageBatchOptions())
			require.NoError(t, err)

			waitChangeRestBufferSizeBytes(e.reader, bufferSizeAfterReadOneMessage)
			require.Equal(t, e.initialBufferSizeBytes, e.reader.restBufferSizeBytes.Load())
		})

		xtest.TestManyTimesWithName(t, "ForceReturnBatchIfBufferFull", func(t testing.TB) {
			e := newTopicReaderTestEnv(t)

			dataRequested := make(empty.Chan)
			e.stream.EXPECT().Send(
				&rawtopicreader.ReadRequest{BytesSize: int(e.initialBufferSizeBytes)},
			).
				DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
					close(dataRequested)

					return nil
				})

			e.Start()
			waitChangeRestBufferSizeBytes(e.reader, 0)

			e.SendFromServer(&rawtopicreader.ReadResponse{
				BytesSize: int(e.initialBufferSizeBytes),
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID,
						Batches: []rawtopicreader.Batch{
							{
								MessageData: []rawtopicreader.MessageData{
									{
										Offset: 1,
										SeqNo:  1,
										Data:   []byte{1, 2, 3},
									},
								},
							},
						},
					},
				},
			})
			needReadTwoMessages := newReadMessageBatchOptions()
			needReadTwoMessages.MinCount = 2

			readTimeoutCtx, cancel := xcontext.WithTimeout(e.ctx, time.Second)
			defer cancel()

			batch, err := e.reader.ReadMessageBatch(readTimeoutCtx, needReadTwoMessages)
			require.NoError(t, err)
			require.Len(t, batch.Messages, 1)

			<-dataRequested
		})
	})

	xtest.TestManyTimesWithName(t, "ReadBatch", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		compress := func(msg string) []byte {
			b := &bytes.Buffer{}
			writer := gzip.NewWriter(b)
			_, err := writer.Write([]byte(msg))
			require.NoError(t, writer.Close())
			require.NoError(t, err)

			return b.Bytes()
		}

		prevOffset := e.partitionSession.LastReceivedMessageOffset()

		sendDataRequestCompleted := make(empty.Chan)
		dataSize := 6
		e.stream.EXPECT().Send(
			&rawtopicreader.ReadRequest{BytesSize: dataSize},
		).DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
			close(sendDataRequestCompleted)

			return nil
		})
		e.SendFromServer(&rawtopicreader.ReadResponse{
			BytesSize: dataSize,
			PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: e.partitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							Codec:            rawtopiccommon.CodecRaw,
							WriteSessionMeta: map[string]string{"a": "b", "c": "d"},
							WrittenAt:        testTime(5),
							MessageData: []rawtopicreader.MessageData{
								{
									Offset:           prevOffset + 1,
									SeqNo:            1,
									CreatedAt:        testTime(1),
									Data:             []byte("123"),
									UncompressedSize: 3,
									MessageGroupID:   "1",
								},
								{
									Offset:           prevOffset + 2,
									SeqNo:            2,
									CreatedAt:        testTime(2),
									Data:             []byte("4567"),
									UncompressedSize: 4,
									MessageGroupID:   "1",
								},
							},
						},
						{
							Codec:            rawtopiccommon.CodecGzip,
							WriteSessionMeta: map[string]string{"e": "f", "g": "h"},
							WrittenAt:        testTime(6),
							MessageData: []rawtopicreader.MessageData{
								{
									Offset:           prevOffset + 10,
									SeqNo:            3,
									CreatedAt:        testTime(3),
									Data:             compress("098"),
									UncompressedSize: 3,
									MessageGroupID:   "2",
								},
								{
									Offset:           prevOffset + 20,
									SeqNo:            4,
									CreatedAt:        testTime(4),
									Data:             compress("0987"),
									UncompressedSize: 4,
									MessageGroupID:   "2",
								},
							},
						},
						{
							Codec:            rawtopiccommon.CodecRaw,
							WriteSessionMeta: map[string]string{"a": "b", "c": "d"},
							WrittenAt:        testTime(7),
							MessageData: []rawtopicreader.MessageData{
								{
									Offset:           prevOffset + 30,
									SeqNo:            5,
									CreatedAt:        testTime(5),
									Data:             []byte("test"),
									UncompressedSize: 4,
									MessageGroupID:   "1",
									MetadataItems: []rawtopiccommon.MetadataItem{
										{
											Key:   "first",
											Value: []byte("first-value"),
										},
										{
											Key:   "second",
											Value: []byte("second-value"),
										},
									},
								},
								{
									Offset:           prevOffset + 31,
									SeqNo:            6,
									CreatedAt:        testTime(5),
									Data:             []byte("4567"),
									UncompressedSize: 4,
									MessageGroupID:   "1",
									MetadataItems: []rawtopiccommon.MetadataItem{
										{
											Key:   "doubled-key",
											Value: []byte("bad"),
										},
										{
											Key:   "doubled-key",
											Value: []byte("good"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		)

		expectedData := [][]byte{[]byte("123"), []byte("4567"), []byte("098"), []byte("0987"), []byte("test"), []byte("4567")}
		expectedBatch := topicreadercommon.BatchSetCommitRangeForTest(&topicreadercommon.PublicBatch{
			Messages: []*topicreadercommon.PublicMessage{
				topicreadercommon.NewPublicMessageBuilder().
					Seqno(1).
					CreatedAt(testTime(1)).
					MessageGroupID("1").
					Offset(prevOffset.ToInt64() + 1).
					WrittenAt(testTime(5)).
					WriteSessionMetadata(map[string]string{"a": "b", "c": "d"}).
					UncompressedSize(3).
					RawDataLen(3).
					CommitRange(topicreadercommon.CommitRange{
						CommitOffsetStart: prevOffset + 1,
						CommitOffsetEnd:   prevOffset + 2,
						PartitionSession:  e.partitionSession,
					}).Build(),
				topicreadercommon.NewPublicMessageBuilder().
					Seqno(2).
					CreatedAt(testTime(2)).
					MessageGroupID("1").
					Offset(prevOffset.ToInt64() + 2).
					WrittenAt(testTime(5)).
					WriteSessionMetadata(map[string]string{"a": "b", "c": "d"}).
					UncompressedSize(4).
					RawDataLen(4).
					CommitRange(topicreadercommon.CommitRange{
						CommitOffsetStart: prevOffset + 2,
						CommitOffsetEnd:   prevOffset + 3,
						PartitionSession:  e.partitionSession,
					}).Build(),
				topicreadercommon.NewPublicMessageBuilder().
					Seqno(3).
					CreatedAt(testTime(3)).
					MessageGroupID("2").
					Offset(prevOffset.ToInt64() + 10).
					WrittenAt(testTime(6)).
					WriteSessionMetadata(map[string]string{"e": "f", "g": "h"}).
					UncompressedSize(3).
					RawDataLen(len(compress("098"))).
					CommitRange(topicreadercommon.CommitRange{
						CommitOffsetStart: prevOffset + 3,
						CommitOffsetEnd:   prevOffset + 11,
						PartitionSession:  e.partitionSession,
					}).Build(),
				topicreadercommon.NewPublicMessageBuilder().
					Seqno(4).
					CreatedAt(testTime(4)).
					MessageGroupID("2").
					Offset(prevOffset.ToInt64() + 20).
					WrittenAt(testTime(6)).
					WriteSessionMetadata(map[string]string{"e": "f", "g": "h"}).
					UncompressedSize(4).
					RawDataLen(len(compress("0987"))).
					CommitRange(topicreadercommon.CommitRange{
						CommitOffsetStart: prevOffset + 11,
						CommitOffsetEnd:   prevOffset + 21,
						PartitionSession:  e.partitionSession,
					}).Build(),
				topicreadercommon.NewPublicMessageBuilder().
					Seqno(5).
					CreatedAt(testTime(5)).
					MessageGroupID("1").
					Metadata(map[string][]byte{
						"first":  []byte("first-value"),
						"second": []byte("second-value"),
					}).
					Offset(prevOffset.ToInt64() + 30).
					WrittenAt(testTime(7)).
					WriteSessionMetadata(map[string]string{"a": "b", "c": "d"}).
					UncompressedSize(4).
					RawDataLen(4).
					CommitRange(topicreadercommon.CommitRange{
						CommitOffsetStart: prevOffset + 21,
						CommitOffsetEnd:   prevOffset + 31,
						PartitionSession:  e.partitionSession,
					}).Build(),
				topicreadercommon.NewPublicMessageBuilder().
					Seqno(6).
					CreatedAt(testTime(5)).
					MessageGroupID("1").
					Metadata(map[string][]byte{
						"doubled-key": []byte("good"),
					}).
					Offset(prevOffset.ToInt64() + 31).
					WrittenAt(testTime(7)).
					WriteSessionMetadata(map[string]string{"a": "b", "c": "d"}).
					UncompressedSize(4).
					RawDataLen(4).
					CommitRange(topicreadercommon.CommitRange{
						CommitOffsetStart: prevOffset + 31,
						CommitOffsetEnd:   prevOffset + 32,
						PartitionSession:  e.partitionSession,
					}).Build(),
			},
		}, topicreadercommon.CommitRange{
			CommitOffsetStart: prevOffset + 1,
			CommitOffsetEnd:   prevOffset + 32,
			PartitionSession:  e.partitionSession,
		})

		opts := newReadMessageBatchOptions()
		opts.MinCount = 6
		batch, err := e.reader.ReadMessageBatch(e.ctx, opts)
		require.NoError(t, err)

		data := make([][]byte, 0, len(batch.Messages))
		for i := range batch.Messages {
			content, err := io.ReadAll(batch.Messages[i])
			require.NoError(t, err)
			data = append(data, content)
			topicreadercommon.MessageSetNilDataForTest(batch.Messages[i])
		}

		require.Equal(t, expectedData, data)
		require.Equal(t, expectedBatch, batch)
		<-sendDataRequestCompleted
	})
}

func TestTopicStreamReadImpl_BatchReaderWantMoreMessagesThenBufferCanHold(t *testing.T) {
	sendMessageWithFullBuffer := func(e *streamEnv) empty.Chan {
		nextDataRequested := make(empty.Chan)
		e.stream.EXPECT().Send(
			&rawtopicreader.ReadRequest{BytesSize: int(e.initialBufferSizeBytes)},
		).DoAndReturn(func(_ rawtopicreader.ClientMessage) error {
			close(nextDataRequested)

			return nil
		})

		e.SendFromServer(
			&rawtopicreader.ReadResponse{
				BytesSize: int(e.initialBufferSizeBytes),
				PartitionData: []rawtopicreader.PartitionData{
					{
						PartitionSessionID: e.partitionSessionID,
						Batches: []rawtopicreader.Batch{
							{
								Codec: rawtopiccommon.CodecRaw,
								MessageData: []rawtopicreader.MessageData{
									{
										Offset: 1,
									},
								},
							},
						},
					},
				},
			})

		return nextDataRequested
	}

	xtest.TestManyTimesWithName(t, "ReadAfterMessageInBuffer", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		nextDataRequested := sendMessageWithFullBuffer(&e)

		// wait message received to internal buffer
		xtest.SpinWaitCondition(t, &e.reader.batcher.m, func() bool {
			return len(e.reader.batcher.messages) > 0
		})

		xtest.SpinWaitCondition(t, nil, func() bool {
			return e.reader.restBufferSizeBytes.Load() == 0
		})

		opts := newReadMessageBatchOptions()
		opts.MinCount = 2

		readCtx, cancel := xcontext.WithTimeout(e.ctx, time.Second)
		defer cancel()
		batch, err := e.reader.ReadMessageBatch(readCtx, opts)
		require.NoError(t, err)
		require.Len(t, batch.Messages, 1)
		require.Equal(t, int64(1), batch.Messages[0].Offset)

		<-nextDataRequested
		require.Equal(t, e.initialBufferSizeBytes, e.reader.restBufferSizeBytes.Load())
	})

	xtest.TestManyTimesWithName(t, "ReadBeforeMessageInBuffer", func(t testing.TB) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		readCompleted := make(empty.Chan)
		var batch *topicreadercommon.PublicBatch
		var readErr error
		go func() {
			defer close(readCompleted)

			opts := newReadMessageBatchOptions()
			opts.MinCount = 2

			readCtx, cancel := xcontext.WithTimeout(e.ctx, time.Second)
			defer cancel()
			batch, readErr = e.reader.ReadMessageBatch(readCtx, opts)
		}()

		// wait to start pop
		e.reader.batcher.notifyAboutNewMessages()
		xtest.SpinWaitCondition(t, &e.reader.batcher.m, func() bool {
			return len(e.reader.batcher.hasNewMessages) == 0
		})

		nextDataRequested := sendMessageWithFullBuffer(&e)

		<-readCompleted
		require.NoError(t, readErr)
		require.Len(t, batch.Messages, 1)
		require.Equal(t, int64(1), batch.Messages[0].Offset)

		<-nextDataRequested
		require.Equal(t, e.initialBufferSizeBytes, e.reader.restBufferSizeBytes.Load())
	})
}

func TestTopicStreamReadImpl_CommitWithBadSession(t *testing.T) {
	commitByMode := func(mode topicreadercommon.PublicCommitMode) error {
		sleep := func() {
			time.Sleep(time.Second / 10)
		}
		e := newTopicReaderTestEnv(t)
		e.reader.cfg.CommitMode = mode
		e.Start()

		cr := topicreadercommon.CommitRange{
			PartitionSession: topicreadercommon.NewPartitionSession(
				context.Background(),
				"asd",
				123,
				topicreadercommon.NextReaderID(),
				"bad-connection-id",
				222,
				322,
				213,
			),
		}
		commitErr := e.reader.Commit(e.ctx, cr)

		sleep()

		require.False(t, e.reader.closed)

		return commitErr
	}
	t.Run("CommitModeNone", func(t *testing.T) {
		require.ErrorIs(
			t,
			commitByMode(topicreadercommon.CommitModeNone),
			topicreadercommon.ErrCommitDisabled,
		)
	})
	t.Run("CommitModeSync", func(t *testing.T) {
		require.ErrorIs(
			t,
			commitByMode(topicreadercommon.CommitModeSync),
			topicreadercommon.ErrPublicCommitSessionToExpiredSession,
		)
	})
	t.Run("CommitModeAsync", func(t *testing.T) {
		require.NoError(t, commitByMode(topicreadercommon.CommitModeAsync))
	})
}

type streamEnv struct {
	TopicClient             *MockTopicClient
	ctx                     context.Context //nolint:containedctx
	t                       testing.TB
	reader                  *topicStreamReaderImpl
	stopReadEvents          empty.Chan
	stopReadEventsCloseOnce sync.Once
	stream                  *MockRawTopicReaderStream
	partitionSessionID      partitionSessionID
	mc                      *gomock.Controller
	partitionSession        *topicreadercommon.PartitionSession
	initialBufferSizeBytes  int64

	m                          xsync.Mutex
	messagesFromServerToClient chan testStreamResult
	nextMessageNeedCallback    func()
}

type testStreamResult struct {
	nextMessageCallback func()
	msg                 rawtopicreader.ServerMessage
	err                 error
	waitOnly            bool
}

func newTopicReaderTestEnv(t testing.TB) streamEnv {
	ctx := xtest.Context(t)

	mc := gomock.NewController(t)

	stream := NewMockRawTopicReaderStream(mc)

	const initialBufferSizeBytes = 1000000

	cfg := newTopicStreamReaderConfig()
	cfg.BaseContext = ctx
	cfg.BufferSizeProtoBytes = initialBufferSizeBytes
	cfg.CommitterBatchTimeLag = 0

	topicClientMock := NewMockTopicClient(mc)
	reader := newTopicStreamReaderStopped(topicClientMock, topicreadercommon.NextReaderID(), stream, cfg)
	// reader.initSession() - skip stream level initialization

	const testPartitionID = 5
	const testSessionID = 15
	const testClientSessionID = 115
	const testSessionComitted = 20

	session := topicreadercommon.NewPartitionSession(
		ctx,
		"/test",
		testPartitionID,
		reader.readerID,
		reader.readConnectionID,
		testSessionID,
		testClientSessionID,
		testSessionComitted,
	)
	require.NoError(t, reader.sessionController.Add(session))

	env := streamEnv{
		TopicClient:                topicClientMock,
		ctx:                        ctx,
		t:                          t,
		initialBufferSizeBytes:     initialBufferSizeBytes,
		reader:                     reader,
		stopReadEvents:             make(empty.Chan),
		stream:                     stream,
		messagesFromServerToClient: make(chan testStreamResult),
		partitionSession:           session,
		partitionSessionID:         session.StreamPartitionSessionID,
		mc:                         mc,
	}

	stream.EXPECT().Recv().AnyTimes().DoAndReturn(env.receiveMessageHandler)

	// initial data request
	stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: initialBufferSizeBytes}).MaxTimes(1)

	// allow in test send data without explicit sizes
	stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: 0}).AnyTimes()

	streamClosed := make(empty.Chan)
	stream.EXPECT().CloseSend().Return(nil).DoAndReturn(func() error {
		close(streamClosed)
		env.closeStream()

		return nil
	})

	t.Cleanup(func() {
		env.closeStream()
		_ = env.reader.CloseWithError(ctx, errors.New("test finished"))
		xtest.WaitChannelClosed(t, streamClosed)
	})

	t.Cleanup(func() {
		if messLen := len(env.messagesFromServerToClient); messLen != 0 {
			t.Fatalf("not all messages consumed from server: %v", messLen)
		}
	})

	//nolint:govet
	return env
}

func (e *streamEnv) Start() {
	require.NoError(e.t, e.reader.startBackgroundWorkers())
	xtest.SpinWaitCondition(e.t, nil, func() bool {
		return e.reader.restBufferSizeBytes.Load() == e.initialBufferSizeBytes
	})
}

func (e *streamEnv) readerReceiveWaitClose(callback func()) {
	e.stream.EXPECT().Recv().DoAndReturn(func() (rawtopicreader.ServerMessage, error) {
		if callback != nil {
			callback()
		}
		<-e.ctx.Done()

		return nil, errors.New("test reader closed")
	})
}

func (e *streamEnv) SendFromServer(msg rawtopicreader.ServerMessage) {
	e.SendFromServerAndSetNextCallback(msg, nil)
}

func (e *streamEnv) SendFromServerAndSetNextCallback(msg rawtopicreader.ServerMessage, callback func()) {
	if msg.StatusData().Status == 0 {
		msg.SetStatus(rawydb.StatusSuccess)
	}
	e.messagesFromServerToClient <- testStreamResult{msg: msg, nextMessageCallback: callback}
}

func (e *streamEnv) WaitMessageReceived() {
	e.messagesFromServerToClient <- testStreamResult{waitOnly: true}
}

func (e *streamEnv) receiveMessageHandler() (rawtopicreader.ServerMessage, error) {
	if e.ctx.Err() != nil {
		return nil, e.ctx.Err()
	}

	var callback func()
	e.m.WithLock(func() {
		callback = e.nextMessageNeedCallback
		e.nextMessageNeedCallback = nil
	})

	if callback != nil {
		callback()
	}

readMessages:
	for {
		select {
		case <-e.ctx.Done():
			return nil, e.ctx.Err()
		case <-e.stopReadEvents:
			return nil, xerrors.Wrap(errors.New("mock reader closed"))
		case res := <-e.messagesFromServerToClient:
			if res.waitOnly {
				continue readMessages
			}
			e.m.WithLock(func() {
				e.nextMessageNeedCallback = res.nextMessageCallback
			})

			return res.msg, res.err
		}
	}
}

func (e *streamEnv) closeStream() {
	e.stopReadEventsCloseOnce.Do(func() {
		close(e.stopReadEvents)
	})
}

func TestUpdateCommitInTransaction(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		initialCommitOffset := e.partitionSession.CommittedOffset()
		txID := "test-tx-id"
		sessionID := "test-session-id"

		e.TopicClient.EXPECT().UpdateOffsetsInTransaction(gomock.Any(), &rawtopic.UpdateOffsetsInTransactionRequest{
			OperationParams: rawydb.OperationParams{
				OperationMode: rawydb.OperationParamsModeSync,
			},
			Tx: rawtopiccommon.TransactionIdentity{
				ID:      txID,
				Session: sessionID,
			},
			Topics: []rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets{
				{
					Path: e.partitionSession.Topic,
					Partitions: []rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets{
						{
							PartitionID: e.partitionSession.PartitionID,
							PartitionOffsets: []rawtopiccommon.OffsetRange{
								{
									Start: initialCommitOffset,
									End:   initialCommitOffset + 1,
								},
							},
						},
					},
				},
			},
			Consumer: e.reader.cfg.Consumer,
		})

		txMock := newMockTransactionWrapper(sessionID, txID)

		batch, err := topicreadercommon.NewBatch(e.partitionSession, []*topicreadercommon.PublicMessage{
			topicreadercommon.NewPublicMessageBuilder().
				Offset(e.partitionSession.CommittedOffset().ToInt64()).
				PartitionSession(e.partitionSession).
				Build(),
		})
		require.NoError(t, err)
		err = e.reader.commitWithTransaction(e.ctx, txMock, batch)
		require.NoError(t, err)

		require.Len(t, txMock.onCompleted, 1)
		txMock.onCompleted[0](nil)
		require.True(t, txMock.materialized)
		require.Equal(t, initialCommitOffset+1, e.partitionSession.CommittedOffset())
	})
	t.Run("FailedAddCommitToTransactions", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		txID := "test-tx-id"
		sessionID := "test-session-id"

		testError := errors.New("test error")
		e.TopicClient.EXPECT().UpdateOffsetsInTransaction(gomock.Any(), gomock.Any()).Return(testError)

		txMock := newMockTransactionWrapper(sessionID, txID)

		batch, err := topicreadercommon.NewBatch(e.partitionSession, []*topicreadercommon.PublicMessage{
			topicreadercommon.NewPublicMessageBuilder().
				Offset(e.partitionSession.CommittedOffset().ToInt64()).
				PartitionSession(e.partitionSession).
				Build(),
		})
		require.NoError(t, err)
		err = e.reader.commitWithTransaction(e.ctx, txMock, batch)
		require.ErrorIs(t, err, testError)
		require.NoError(t, xerrors.RetryableError(err))
		require.Empty(t, txMock.onCompleted)

		require.True(t, e.reader.closed)
		require.ErrorIs(t, e.reader.err, testError)
		require.Error(t, xerrors.RetryableError(e.reader.err))
		require.True(t, txMock.RolledBack)
		require.True(t, txMock.materialized)
	})
}
