package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestStreamListener_OnReceiveServerMessage(t *testing.T) {
	const batchBytes = 100

	const seqNo int64 = 4

	xtest.TestManyTimesWithName(t, "onReadResponse", func(t testing.TB) {
		e := fixenv.New(t)
		ctx := sf.Context(e)

		defer func() {
			req := StreamListener(e).messagesToSend[0]
			require.Equal(t, batchBytes, req.(*rawtopicreader.ReadRequest).BytesSize)
		}()

		EventHandlerMock(e).EXPECT().OnReadMessages(PartitionSession(e).Context(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
				require.Equal(t, PartitionSession(e).ClientPartitionSessionID, event.PartitionSession.PartitionSessionID)
				require.Equal(t, seqNo, event.Batch.Messages[0].SeqNo)

				return nil
			})

		StreamListener(e).onReceiveServerMessage(ctx, &rawtopicreader.ReadResponse{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
				Status: rawydb.StatusSuccess,
			},
			BytesSize: batchBytes,
			PartitionData: []rawtopicreader.PartitionData{
				{
					PartitionSessionID: PartitionSession(e).StreamPartitionSessionID,
					Batches: []rawtopicreader.Batch{
						{
							Codec:            rawtopiccommon.CodecRaw,
							ProducerID:       "test-producer",
							WriteSessionMeta: nil,
							MessageData: []rawtopicreader.MessageData{
								{
									Offset:           PartitionSession(e).CommittedOffset(),
									SeqNo:            seqNo,
									CreatedAt:        testTime(0),
									Data:             []byte("123"),
									UncompressedSize: 3,
									MessageGroupID:   "mess-group-id",
									MetadataItems:    nil,
								},
							},
						},
					},
				},
			},
		})
	})
	xtest.TestManyTimesWithName(t, "onStartPartitionSession", func(t testing.TB) {
		e := fixenv.New(t)

		respReadOffset := int64(16)
		respCommitOffset := int64(25)

		EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, event *PublicStartPartitionSessionEvent) error {
			require.Equal(t, topicreadercommon.PublicPartitionSession{
				PartitionSessionID: 1, // ClientPartitionSessionID
				TopicPath:          "asd",
				PartitionID:        123,
			}, event.PartitionSession)
			require.Equal(t, int64(10), event.CommittedOffset)
			require.Equal(t, PublicOffsetsRange{
				Start: 5,
				End:   15,
			}, event.PartitionOffsets)
			event.ConfirmWithParams(PublicStartPartitionSessionConfirm{}.
				WithReadOffet(respReadOffset).
				WithCommitOffset(respCommitOffset),
			)

			return nil
		})

		StreamListener(e).onReceiveServerMessage(sf.Context(e), &rawtopicreader.StartPartitionSessionRequest{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
				Status: rawydb.StatusSuccess,
			},
			PartitionSession: rawtopicreader.PartitionSession{
				PartitionSessionID: 100,
				Path:               "asd",
				PartitionID:        123,
			},
			CommittedOffset: 10,
			PartitionOffsets: rawtopicreader.OffsetRange{
				Start: 5,
				End:   15,
			},
		})

		req := StreamListener(e).messagesToSend[0]
		require.Equal(t, &rawtopicreader.StartPartitionSessionResponse{
			PartitionSessionID: 100,
			ReadOffset: rawtopicreader.OptionalOffset{
				Offset:   rawtopicreader.NewOffset(respReadOffset),
				HasValue: true,
			},
			CommitOffset: rawtopicreader.OptionalOffset{
				Offset:   rawtopicreader.NewOffset(respCommitOffset),
				HasValue: true,
			},
		}, req)

		session, err := StreamListener(e).sessions.Get(100)
		require.NoError(t, err)
		require.NotNil(t, session)
	})
	xtest.TestManyTimesWithName(t, "onStopPartitionRequest", func(t testing.TB) {
		e := fixenv.New(t)
		ctx := sf.Context(e)

		listener := StreamListener(e)

		EventHandlerMock(e).EXPECT().OnStopPartitionSessionRequest(
			PartitionSession(e).Context(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, event *PublicStopPartitionSessionEvent) error {
			require.Equal(t, PartitionSession(e).ClientPartitionSessionID, event.PartitionSession.PartitionSessionID)
			require.True(t, event.Graceful)
			require.Equal(t, int64(5), event.CommittedOffset)
			event.Confirm()

			return nil
		})

		listener.onReceiveServerMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
				Status: rawydb.StatusSuccess,
			},
			PartitionSessionID: PartitionSession(e).StreamPartitionSessionID,
			Graceful:           true,
			CommittedOffset:    5,
		})

		req := listener.messagesToSend[0]
		require.Equal(
			t,
			&rawtopicreader.StopPartitionSessionResponse{
				PartitionSessionID: PartitionSession(e).StreamPartitionSessionID,
			},
			req,
		)
	})
}

func TestStreamListener_CloseSessionsOnCloseListener(t *testing.T) {
	e := fixenv.New(t)
	EventHandlerMock(e).EXPECT().OnStopPartitionSessionRequest(
		PartitionSession(e).Context(),
		gomock.Any(),
	).Do(func(ctx context.Context, event *PublicStopPartitionSessionEvent) error {
		require.Equal(t, PartitionSession(e).ClientPartitionSessionID, event.PartitionSession.PartitionSessionID)
		require.False(t, event.Graceful)
		require.Equal(t, PartitionSession(e).CommittedOffset().ToInt64(), event.CommittedOffset)
		event.Confirm()

		return nil
	})
	require.NoError(t, StreamListener(e).Close(sf.Context(e), errors.New("test")))
}

func testTime(num int) time.Time {
	return time.Date(2000, 1, 1, 0, 0, num, 0, time.UTC)
}
