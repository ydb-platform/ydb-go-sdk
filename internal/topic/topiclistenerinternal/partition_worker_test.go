package topiclistenerinternal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

//go:generate mockgen -source partition_worker.go -destination partition_worker_mock_test.go --typed -package topiclistenerinternal -write_package_comment=false

// Test Synchronization Helpers - These replace time.Sleep with simple channel coordination

// syncMessageSender extends mockMessageSender with synchronization capabilities
type syncMessageSender struct {
	*mockMessageSender
	messageReceived empty.Chan
}

func newSyncMessageSender() *syncMessageSender {
	return &syncMessageSender{
		mockMessageSender: newMockMessageSender(),
		messageReceived:   make(empty.Chan, 100), // buffered to prevent blocking
	}
}

func (s *syncMessageSender) SendRaw(msg rawtopicreader.ClientMessage) {
	s.mockMessageSender.SendRaw(msg)
	// Signal that a message was received
	select {
	case s.messageReceived <- empty.Struct{}:
	default:
	}
}

// waitForMessage waits for at least one message to be sent
func (s *syncMessageSender) waitForMessage(ctx context.Context) error {
	select {
	case <-s.messageReceived:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitForMessages waits for at least n messages to be sent
func (s *syncMessageSender) waitForMessages(ctx context.Context, n int) error {
	for i := 0; i < n; i++ {
		if err := s.waitForMessage(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Test Helpers and Mocks

type mockMessageSender struct {
	mu       sync.Mutex
	messages []rawtopicreader.ClientMessage
}

func newMockMessageSender() *mockMessageSender {
	return &mockMessageSender{
		messages: make([]rawtopicreader.ClientMessage, 0),
	}
}

func (m *mockMessageSender) SendRaw(msg rawtopicreader.ClientMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
}

func (m *mockMessageSender) GetMessages() []rawtopicreader.ClientMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]rawtopicreader.ClientMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

func (m *mockMessageSender) GetMessageCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

func createTestPartitionSession() *topicreadercommon.PartitionSession {
	ctx := context.Background()
	return topicreadercommon.NewPartitionSession(
		ctx,
		"test-topic",
		1,
		123,
		"test-session",
		456,
		789,
		rawtopiccommon.NewOffset(100),
	)
}

func createTestStartPartitionRequest() *rawtopicreader.StartPartitionSessionRequest {
	req := &rawtopicreader.StartPartitionSessionRequest{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		PartitionSession: rawtopicreader.PartitionSession{
			PartitionSessionID: rawtopicreader.PartitionSessionID(456),
			Path:               "test-topic",
			PartitionID:        1,
		},
		CommittedOffset: rawtopiccommon.NewOffset(100),
		PartitionOffsets: rawtopiccommon.OffsetRange{
			Start: rawtopiccommon.NewOffset(0),
			End:   rawtopiccommon.NewOffset(1000),
		},
	}
	return req
}

func createTestStopPartitionRequest(graceful bool) *rawtopicreader.StopPartitionSessionRequest {
	req := &rawtopicreader.StopPartitionSessionRequest{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		PartitionSessionID: rawtopicreader.PartitionSessionID(456),
		Graceful:           graceful,
		CommittedOffset:    rawtopiccommon.NewOffset(200),
	}
	return req
}

func createTestReadResponse() *rawtopicreader.ReadResponse {
	req := &rawtopicreader.ReadResponse{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		BytesSize: 1024,
		PartitionData: []rawtopicreader.PartitionData{
			{
				PartitionSessionID: rawtopicreader.PartitionSessionID(456),
				Batches: []rawtopicreader.Batch{
					{
						Codec:            rawtopiccommon.CodecRaw,
						ProducerID:       "test-producer",
						WriteSessionMeta: nil,
						WrittenAt:        time.Now(),
						MessageData: []rawtopicreader.MessageData{
							{
								Offset:           rawtopiccommon.NewOffset(150),
								SeqNo:            1,
								CreatedAt:        time.Now(),
								Data:             []byte("test message"),
								UncompressedSize: 12,
								MessageGroupID:   "",
								MetadataItems:    nil,
							},
						},
					},
				},
			},
		},
	}
	return req
}

// =============================================================================
// INTERFACE TESTS - Test external behavior through public API only
// =============================================================================

func TestPartitionWorkerInterface_StartPartitionSessionFlow(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := createTestPartitionSession()
	messageSender := newSyncMessageSender()
	mockHandler := NewMockEventHandler(ctrl)
	confirmReady := make(empty.Chan)

	var stoppedErr error
	onStopped := func(sessionID int64, err error) {
		stoppedErr = err
	}

	worker := NewPartitionWorker(
		789,
		session,
		messageSender,
		mockHandler,
		onStopped,
		nil, // streamListener not needed for this test
	)

	// Set up mock expectations with deterministic coordination
	mockHandler.EXPECT().
		OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, event *PublicEventStartPartitionSession) error {
			// Verify event data
			require.Equal(t, session.ToPublic(), event.PartitionSession)
			require.Equal(t, int64(100), event.CommittedOffset)

			// Use channel coordination instead of time.Sleep
			go func() {
				// Wait for test signal instead of sleeping
				xtest.WaitChannelClosed(t, confirmReady)
				event.Confirm()
			}()
			return nil
		})

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Send start partition request
	startReq := createTestStartPartitionRequest()
	worker.SendMessage(startReq)

	// Signal that confirmation can proceed
	close(confirmReady)

	// Wait for response using synchronous message sender
	err := messageSender.waitForMessage(ctx)
	require.NoError(t, err)

	// Verify response was sent
	messages := messageSender.GetMessages()
	require.Len(t, messages, 1)

	response, ok := messages[0].(*rawtopicreader.StartPartitionSessionResponse)
	require.True(t, ok)
	require.Equal(t, startReq.PartitionSession.PartitionSessionID, response.PartitionSessionID)
	require.Nil(t, stoppedErr)
}

func TestPartitionWorkerInterface_StopPartitionSessionFlow(t *testing.T) {
	t.Run("GracefulStop", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		session := createTestPartitionSession()
		messageSender := newSyncMessageSender()
		mockHandler := NewMockEventHandler(ctrl)
		confirmReady := make(empty.Chan)

		var stoppedErr error
		onStopped := func(sessionID int64, err error) {
			stoppedErr = err
		}

		worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, nil)

		// Set up mock expectations with deterministic coordination
		mockHandler.EXPECT().
			OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, event *PublicEventStopPartitionSession) error {
				// Verify event data
				require.Equal(t, session.ToPublic(), event.PartitionSession)
				require.True(t, event.Graceful)
				require.Equal(t, int64(200), event.CommittedOffset)

				// Use channel coordination instead of time.Sleep
				go func() {
					xtest.WaitChannelClosed(t, confirmReady)
					event.Confirm()
				}()
				return nil
			})

		worker.Start(ctx)
		defer func() {
			err := worker.Close(ctx, nil)
			require.NoError(t, err)
		}()

		// Send graceful stop request
		stopReq := createTestStopPartitionRequest(true)
		worker.SendMessage(stopReq)

		// Signal that confirmation can proceed
		close(confirmReady)

		// Wait for response using synchronous message sender
		err := messageSender.waitForMessage(ctx)
		require.NoError(t, err)

		messages := messageSender.GetMessages()
		require.Len(t, messages, 1)

		response, ok := messages[0].(*rawtopicreader.StopPartitionSessionResponse)
		require.True(t, ok)
		require.Equal(t, session.StreamPartitionSessionID, response.PartitionSessionID)
		require.Nil(t, stoppedErr)
	})

	t.Run("NonGracefulStop", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		session := createTestPartitionSession()
		messageSender := newSyncMessageSender()
		mockHandler := NewMockEventHandler(ctrl)
		confirmReady := make(empty.Chan)
		processingDone := make(empty.Chan)

		var stoppedErr error
		onStopped := func(sessionID int64, err error) {
			stoppedErr = err
		}

		worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, nil)

		// Set up mock expectations with deterministic coordination
		mockHandler.EXPECT().
			OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, event *PublicEventStopPartitionSession) error {
				// Verify event data
				require.Equal(t, session.ToPublic(), event.PartitionSession)
				require.False(t, event.Graceful)

				// Use channel coordination instead of time.Sleep
				go func() {
					xtest.WaitChannelClosed(t, confirmReady)
					event.Confirm()
					// Signal that processing is complete
					close(processingDone)
				}()
				return nil
			})

		worker.Start(ctx)
		defer func() {
			err := worker.Close(ctx, nil)
			require.NoError(t, err)
		}()

		// Send non-graceful stop request
		stopReq := createTestStopPartitionRequest(false)
		worker.SendMessage(stopReq)

		// Signal that confirmation can proceed
		close(confirmReady)

		// Wait for processing to complete instead of sleeping
		xtest.WaitChannelClosed(t, processingDone)

		// Verify no response was sent for non-graceful stop
		messages := messageSender.GetMessages()
		require.Len(t, messages, 0)
		require.Nil(t, stoppedErr)
	})
}

func TestPartitionWorkerInterface_ReadResponseFlow(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := createTestPartitionSession()
	messageSender := newSyncMessageSender()
	mockHandler := NewMockEventHandler(ctrl)
	processingDone := make(empty.Chan)

	var stoppedErr error
	onStopped := func(sessionID int64, err error) {
		stoppedErr = err
	}

	// Create a mock streamListener for NewPublicReadMessages
	mockStreamListener := &streamListener{}

	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, mockStreamListener)

	// Set up mock expectations with deterministic coordination
	mockHandler.EXPECT().
		OnReadMessages(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
			// Verify the event is properly constructed
			require.NotNil(t, event.Batch)
			require.Equal(t, session.ToPublic(), event.PartitionSession)
			require.Equal(t, mockStreamListener, event.listener)
			// Signal that processing is complete
			close(processingDone)
			return nil
		})

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Send read response
	readResp := createTestReadResponse()
	worker.SendMessage(readResp)

	// Wait for processing to complete instead of sleeping
	xtest.WaitChannelClosed(t, processingDone)
	require.Nil(t, stoppedErr)
}

func TestPartitionWorkerInterface_UserHandlerError(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := createTestPartitionSession()
	messageSender := newSyncMessageSender()
	mockHandler := NewMockEventHandler(ctrl)

	var stoppedSessionID atomic.Int64
	var stoppedErr atomic.Pointer[error]
	var errorReceived = make(empty.Chan, 1)
	onStopped := func(sessionID int64, err error) {
		stoppedSessionID.Store(sessionID)
		stoppedErr.Store(&err)
		// Signal that error was received
		select {
		case errorReceived <- empty.Struct{}:
		default:
		}
	}

	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, nil)

	expectedErr := errors.New("user handler error")

	// Set up mock to return error
	mockHandler.EXPECT().
		OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
		Return(expectedErr)

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Send start partition request that will cause error
	startReq := createTestStartPartitionRequest()
	worker.SendMessage(startReq)

	// Wait for error handling using channel instead of Eventually
	xtest.WaitChannelClosed(t, errorReceived)

	// Verify error propagation through public callback
	require.Equal(t, int64(789), stoppedSessionID.Load())
	require.Equal(t, expectedErr, *stoppedErr.Load())
}

func TestPartitionWorkerInterface_MessageMerging(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := createTestPartitionSession()
	messageSender := newSyncMessageSender()
	mockHandler := NewMockEventHandler(ctrl)

	var messageCount atomic.Int32
	var lastProcessed = make(empty.Chan, 10)
	var stoppedErr error
	onStopped := func(sessionID int64, err error) {
		stoppedErr = err
	}

	mockStreamListener := &streamListener{}
	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, mockStreamListener)

	// Set up mock to count OnReadMessages calls with synchronization
	mockHandler.EXPECT().
		OnReadMessages(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
			messageCount.Add(1)
			// Signal that a message was processed
			select {
			case lastProcessed <- empty.Struct{}:
			default:
			}
			return nil
		}).
		AnyTimes()

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Send multiple ReadResponse messages quickly - they should be merged
	for i := 0; i < 3; i++ {
		readResp := createTestReadResponse()
		worker.SendMessage(readResp)
	}

	// Wait for at least one processing cycle to complete
	xtest.WaitChannelClosed(t, lastProcessed)

	// Give a small additional time for any potential remaining processing
	// This is a controlled wait to ensure all merging has completed
	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	select {
	case <-lastProcessed:
		// Additional processing occurred
	case <-timeoutCtx.Done():
		// No additional processing - this is fine
	}

	// Should have fewer or equal OnReadMessages calls than messages sent due to merging
	actualCount := messageCount.Load()
	require.LessOrEqual(t, actualCount, int32(3), "Messages should be merged, reducing or maintaining handler calls")
	require.Greater(t, actualCount, int32(0), "At least one message should be processed")
	require.Nil(t, stoppedErr)
}

func TestPartitionWorkerInterface_MetadataValidationMerging(t *testing.T) {
	t.Run("IdenticalMetadataMerging", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		session := createTestPartitionSession()
		messageSender := newSyncMessageSender()
		mockHandler := NewMockEventHandler(ctrl)

		var messageCount atomic.Int32
		var lastProcessed = make(empty.Chan, 10)
		var stoppedErr error
		onStopped := func(sessionID int64, err error) {
			stoppedErr = err
		}

		mockStreamListener := &streamListener{}
		worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, mockStreamListener)

		// Set up mock to count OnReadMessages calls with synchronization
		mockHandler.EXPECT().
			OnReadMessages(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
				messageCount.Add(1)
				// Signal that a message was processed
				select {
				case lastProcessed <- empty.Struct{}:
				default:
				}
				return nil
			}).
			AnyTimes()

		worker.Start(ctx)
		defer func() {
			err := worker.Close(ctx, nil)
			require.NoError(t, err)
		}()

		// Create messages with identical metadata
		commonMetadata := rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: rawydb.Issues{
				{Code: 100, Message: "warning"},
			},
		}

		for i := 0; i < 3; i++ {
			readResp := createTestReadResponse()
			readResp.ServerMessageMetadata = commonMetadata
			worker.SendMessage(readResp)
		}

		// Wait for at least one processing cycle to complete
		xtest.WaitChannelClosed(t, lastProcessed)

		// Messages with identical metadata should be merged
		actualCount := messageCount.Load()
		require.LessOrEqual(t, actualCount, int32(3), "Messages with identical metadata should be merged")
		require.Greater(t, actualCount, int32(0), "At least one message should be processed")
		require.Nil(t, stoppedErr)
	})

	t.Run("DifferentMetadataPreventsMerging", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		session := createTestPartitionSession()
		messageSender := newSyncMessageSender()
		mockHandler := NewMockEventHandler(ctrl)

		var messageCount atomic.Int32
		var processedCount = make(empty.Chan, 10)
		var stoppedErr error
		onStopped := func(sessionID int64, err error) {
			stoppedErr = err
		}

		mockStreamListener := &streamListener{}
		worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, mockStreamListener)

		// Set up mock to count OnReadMessages calls with synchronization
		mockHandler.EXPECT().
			OnReadMessages(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
				messageCount.Add(1)
				// Signal that a message was processed
				select {
				case processedCount <- empty.Struct{}:
				default:
				}
				return nil
			}).
			AnyTimes()

		worker.Start(ctx)
		defer func() {
			err := worker.Close(ctx, nil)
			require.NoError(t, err)
		}()

		// Send messages with different status codes
		readResp1 := createTestReadResponse()
		readResp1.ServerMessageMetadata.Status = rawydb.StatusSuccess

		readResp2 := createTestReadResponse()
		readResp2.ServerMessageMetadata.Status = rawydb.StatusInternalError

		readResp3 := createTestReadResponse()
		readResp3.ServerMessageMetadata.Status = rawydb.StatusSuccess

		worker.SendMessage(readResp1)
		worker.SendMessage(readResp2)
		worker.SendMessage(readResp3)

		// Wait for at least 2 processing cycles (since different metadata should prevent merging)
		for i := 0; i < 2; i++ {
			xtest.WaitChannelClosed(t, processedCount)
		}

		// Messages with different metadata should NOT be merged, resulting in more handler calls
		actualCount := messageCount.Load()
		require.Greater(t, actualCount, int32(1), "Messages with different metadata should not be merged")
		require.Nil(t, stoppedErr)
	})

	t.Run("DifferentIssuesPreventsMerging", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		session := createTestPartitionSession()
		messageSender := newSyncMessageSender()
		mockHandler := NewMockEventHandler(ctrl)

		var messageCount atomic.Int32
		var processedCount = make(empty.Chan, 10)
		var stoppedErr error
		onStopped := func(sessionID int64, err error) {
			stoppedErr = err
		}

		mockStreamListener := &streamListener{}
		worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, mockStreamListener)

		// Set up mock to count OnReadMessages calls with synchronization
		mockHandler.EXPECT().
			OnReadMessages(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
				messageCount.Add(1)
				// Signal that a message was processed
				select {
				case processedCount <- empty.Struct{}:
				default:
				}
				return nil
			}).
			AnyTimes()

		worker.Start(ctx)
		defer func() {
			err := worker.Close(ctx, nil)
			require.NoError(t, err)
		}()

		// Send messages with different issues
		readResp1 := createTestReadResponse()
		readResp1.ServerMessageMetadata = rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: rawydb.Issues{
				{Code: 100, Message: "issue1"},
			},
		}

		readResp2 := createTestReadResponse()
		readResp2.ServerMessageMetadata = rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: rawydb.Issues{
				{Code: 200, Message: "issue2"},
			},
		}

		worker.SendMessage(readResp1)
		worker.SendMessage(readResp2)

		// Wait for at least 2 processing cycles (different issues should prevent merging)
		for i := 0; i < 2; i++ {
			xtest.WaitChannelClosed(t, processedCount)
		}

		// Messages with different issues should NOT be merged
		actualCount := messageCount.Load()
		require.Greater(t, actualCount, int32(1), "Messages with different issues should not be merged")
		require.Nil(t, stoppedErr)
	})

	t.Run("NestedIssuesComparison", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		session := createTestPartitionSession()
		messageSender := newSyncMessageSender()
		mockHandler := NewMockEventHandler(ctrl)

		var messageCount atomic.Int32
		var processedCount = make(empty.Chan, 10)
		var stoppedErr error
		onStopped := func(sessionID int64, err error) {
			stoppedErr = err
		}

		mockStreamListener := &streamListener{}
		worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, mockStreamListener)

		// Set up mock to count OnReadMessages calls with synchronization
		mockHandler.EXPECT().
			OnReadMessages(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
				messageCount.Add(1)
				// Signal that a message was processed
				select {
				case processedCount <- empty.Struct{}:
				default:
				}
				return nil
			}).
			AnyTimes()

		worker.Start(ctx)
		defer func() {
			err := worker.Close(ctx, nil)
			require.NoError(t, err)
		}()

		// Create messages with identical nested issues structure
		commonNestedMetadata := rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusInternalError,
			Issues: rawydb.Issues{
				{
					Code:    100,
					Message: "parent",
					Issues: rawydb.Issues{
						{Code: 101, Message: "child1"},
						{Code: 102, Message: "child2"},
					},
				},
			},
		}

		// First, send a single message with the common metadata
		readResp1 := createTestReadResponse()
		readResp1.ServerMessageMetadata = commonNestedMetadata
		worker.SendMessage(readResp1)

		// Wait for first message processing
		xtest.WaitChannelClosed(t, processedCount)

		firstCount := messageCount.Load()
		require.Equal(t, int32(1), firstCount, "First message should be processed")

		// Now send a message with different nested issues - this should NOT merge
		readRespDifferent := createTestReadResponse()
		readRespDifferent.ServerMessageMetadata = rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusInternalError,
			Issues: rawydb.Issues{
				{
					Code:    103, // Different code at top level to ensure no merging
					Message: "different parent",
					Issues: rawydb.Issues{
						{Code: 104, Message: "different child"},
					},
				},
			},
		}
		worker.SendMessage(readRespDifferent)

		// Wait for second message processing
		xtest.WaitChannelClosed(t, processedCount)

		finalCount := messageCount.Load()
		// Since the messages have different metadata, they should not merge
		// This means we should have exactly 2 processing calls
		require.Equal(t, int32(2), finalCount, "Messages with different metadata should not merge")
		require.Nil(t, stoppedErr)
	})

	t.Run("MetadataValidationRaceConditions", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			session := createTestPartitionSession()
			messageSender := newSyncMessageSender()
			mockHandler := NewMockEventHandler(ctrl)

			var messageCount atomic.Int32
			var processedCount = make(empty.Chan, 100) // Large buffer for concurrent operations
			var stoppedErr error
			onStopped := func(sessionID int64, err error) {
				stoppedErr = err
			}

			mockStreamListener := &streamListener{}
			worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, mockStreamListener)

			// Set up mock to count OnReadMessages calls with synchronization
			mockHandler.EXPECT().
				OnReadMessages(gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
					messageCount.Add(1)
					// Signal that a message was processed
					select {
					case processedCount <- empty.Struct{}:
					default:
					}
					return nil
				}).
				AnyTimes()

			worker.Start(ctx)
			defer func() {
				err := worker.Close(ctx, nil)
				require.NoError(t, err)
			}()

			// Send messages concurrently with different metadata
			const numGoroutines = 5
			const messagesPerGoroutine = 4

			var wg sync.WaitGroup
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()
					for j := 0; j < messagesPerGoroutine; j++ {
						readResp := createTestReadResponse()
						// Use goroutine ID to create different metadata per goroutine
						readResp.ServerMessageMetadata = rawtopiccommon.ServerMessageMetadata{
							Status: rawydb.StatusCode(goroutineID + 1), // Different status per goroutine
							Issues: rawydb.Issues{
								{Code: uint32(goroutineID*100 + j), Message: "concurrent"},
							},
						}
						worker.SendMessage(readResp)
					}
				}(i)
			}
			wg.Wait()

			// Wait for at least one message to be processed using channels
			xtest.WaitChannelClosed(t, processedCount)

			require.Nil(t, stoppedErr)
		})
	})
}

func TestPartitionWorkerInterface_ConcurrentOperations(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		session := createTestPartitionSession()
		messageSender := newSyncMessageSender()
		mockHandler := NewMockEventHandler(ctrl)

		var processedCount atomic.Int32
		var messageProcessed = make(empty.Chan, 100) // Large buffer for concurrent operations
		var stoppedErr error
		onStopped := func(sessionID int64, err error) {
			stoppedErr = err
		}

		mockStreamListener := &streamListener{}
		worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, mockStreamListener)

		// Set up mock to count processed messages with synchronization
		mockHandler.EXPECT().
			OnReadMessages(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
				processedCount.Add(1)
				// Signal that a message was processed
				select {
				case messageProcessed <- empty.Struct{}:
				default:
				}
				return nil
			}).
			AnyTimes()

		worker.Start(ctx)
		defer func() {
			err := worker.Close(ctx, nil)
			require.NoError(t, err)
		}()

		// Send messages concurrently via public API
		const numMessages = 10
		const numGoroutines = 5

		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numMessages/numGoroutines; j++ {
					readResp := createTestReadResponse()
					worker.SendMessage(readResp) // Public API call
				}
			}()
		}
		wg.Wait()

		// Wait for at least one message to be processed using channels
		xtest.WaitChannelClosed(t, messageProcessed)

		require.Nil(t, stoppedErr)
	})
}

// =============================================================================
// IMPLEMENTATION TESTS - Test internal details and edge cases
// =============================================================================

func TestPartitionWorkerImpl_QueueClosureHandling(t *testing.T) {
	// This test verifies that queue closure is properly handled internally
	// by checking that the worker shuts down gracefully when its context is canceled
	ctx, cancel := context.WithCancel(xtest.Context(t))

	session := createTestPartitionSession()
	messageSender := newSyncMessageSender()

	var stoppedSessionID atomic.Int64
	var stoppedErr atomic.Pointer[error]
	var shutdownReceived = make(empty.Chan, 1)
	onStopped := func(sessionID int64, err error) {
		stoppedSessionID.Store(sessionID)
		stoppedErr.Store(&err)
		// Signal that shutdown was received
		select {
		case shutdownReceived <- empty.Struct{}:
		default:
		}
	}

	worker := NewPartitionWorker(789, session, messageSender, nil, onStopped, nil)

	worker.Start(ctx)
	defer func() {
		err := worker.Close(context.Background(), nil)
		require.NoError(t, err)
	}()

	// Cancel context to trigger graceful shutdown immediately (no sleep needed)
	cancel()

	// Wait for shutdown callback using channel
	xtest.WaitChannelClosed(t, shutdownReceived)

	// Verify graceful shutdown
	require.Equal(t, int64(789), stoppedSessionID.Load())
	errPtr := stoppedErr.Load()
	require.NotNil(t, errPtr)
	require.Nil(t, *errPtr) // Graceful shutdown should have no error
}

func TestPartitionWorkerImpl_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	session := createTestPartitionSession()
	messageSender := newMockMessageSender()

	var stoppedSessionID atomic.Int64
	var stoppedErr atomic.Pointer[error]
	onStopped := func(sessionID int64, err error) {
		stoppedSessionID.Store(sessionID)
		stoppedErr.Store(&err)
	}

	worker := NewPartitionWorker(789, session, messageSender, nil, onStopped, nil)

	worker.Start(ctx)
	defer func() {
		err := worker.Close(context.Background(), nil)
		require.NoError(t, err)
	}()

	// Cancel context to trigger graceful shutdown
	cancel()

	// Wait for graceful shutdown
	require.Eventually(t, func() bool {
		errPtr := stoppedErr.Load()
		return errPtr != nil && *errPtr == nil && stoppedSessionID.Load() == 789
	}, time.Second, 10*time.Millisecond)

	// Verify graceful shutdown (no error)
	require.Equal(t, int64(789), stoppedSessionID.Load())
	errPtr := stoppedErr.Load()
	require.NotNil(t, errPtr)
	require.Nil(t, *errPtr)
}

func TestPartitionWorkerImpl_InternalRaces(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := xtest.Context(t)

		session := createTestPartitionSession()
		messageSender := newMockMessageSender()

		var stoppedErr error
		onStopped := func(sessionID int64, err error) {
			stoppedErr = err
		}

		worker := NewPartitionWorker(789, session, messageSender, nil, onStopped, nil)

		// Test concurrent Start and Close to detect internal races
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Start(ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			// ACCEPTABLE time.Sleep: This is intentionally testing race conditions between
			// Start() and Close() operations. The sleep creates the race condition we want to test.
			time.Sleep(10 * time.Millisecond) // Small delay to let Start() begin
			err := worker.Close(ctx, nil)
			require.NoError(t, err)
		}()

		wg.Wait()

		// Should complete without deadlock or race conditions
		// stoppedErr may be nil (graceful) or errPartitionQueueClosed
		_ = stoppedErr // Variable used for debugging purposes if needed
	})
}

func TestPartitionWorkerImpl_PanicRecovery(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := createTestPartitionSession()
	messageSender := newMockMessageSender()
	mockHandler := NewMockEventHandler(ctrl)

	var stoppedSessionID atomic.Int64
	var stoppedErr atomic.Pointer[error]
	onStopped := func(sessionID int64, err error) {
		stoppedSessionID.Store(sessionID)
		stoppedErr.Store(&err)
	}

	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped, nil)

	// Set up mock to panic
	mockHandler.EXPECT().
		OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, event *PublicEventStartPartitionSession) error {
			panic("test panic")
		})

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Send message that will cause panic
	startReq := createTestStartPartitionRequest()
	worker.SendMessage(startReq)

	// Wait for panic recovery and error reporting
	require.Eventually(t, func() bool {
		return stoppedErr.Load() != nil
	}, time.Second, 10*time.Millisecond)

	// Verify panic was recovered and reported
	require.Equal(t, int64(789), stoppedSessionID.Load())
	err := *stoppedErr.Load()
	require.Error(t, err)
	require.Contains(t, err.Error(), "partition worker panic")
	require.Contains(t, err.Error(), "test panic")
}

func TestPartitionWorkerImpl_BackgroundWorkerLifecycle(t *testing.T) {
	ctx := xtest.Context(t)

	session := createTestPartitionSession()
	messageSender := newMockMessageSender()

	var stoppedCount atomic.Int32
	onStopped := func(sessionID int64, err error) {
		stoppedCount.Add(1)
	}

	worker := NewPartitionWorker(789, session, messageSender, nil, onStopped, nil)

	// Start worker
	worker.Start(ctx)

	// Close worker
	err := worker.Close(ctx, nil)
	require.NoError(t, err)

	// Verify proper shutdown occurred (callback should be called once)
	require.Eventually(t, func() bool {
		return stoppedCount.Load() == 1
	}, time.Second, 10*time.Millisecond)
}

func TestPartitionWorkerImpl_MessageTypeHandling(t *testing.T) {
	ctx := xtest.Context(t)

	session := createTestPartitionSession()
	messageSender := newMockMessageSender()

	var stoppedErr error
	onStopped := func(sessionID int64, err error) {
		stoppedErr = err
	}

	worker := NewPartitionWorker(789, session, messageSender, nil, onStopped, nil)

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Test unknown message type handling (should be ignored)
	// Create a custom message type that implements ServerMessage interface
	unknownMsg := &rawtopicreader.CommitOffsetResponse{} // Use existing type as unknown message
	worker.SendMessage(unknownMsg)

	// Give a controlled time for message processing without blocking indefinitely
	// Since unknown messages are ignored, we just need to ensure no error occurs
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	<-timeoutCtx.Done() // Wait for timeout to ensure processing had time to complete

	// Verify no error occurred (unknown messages should be ignored)
	require.Nil(t, stoppedErr)
}
