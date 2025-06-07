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
	worker.SendRawServerMessage(startReq)

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

		worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped)

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
		worker.SendRawServerMessage(stopReq)

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

		worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped)

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
		worker.SendRawServerMessage(stopReq)

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

func TestPartitionWorkerInterface_BatchMessageFlow(t *testing.T) {
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

	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped)

	// Set up mock expectations with deterministic coordination
	mockHandler.EXPECT().
		OnReadMessages(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
			// Verify the event is properly constructed
			require.NotNil(t, event.Batch)
			require.Equal(t, session.ToPublic(), event.PartitionSession)
			// Signal that processing is complete
			close(processingDone)
			return nil
		})

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Create a test batch and send it via BatchMessage
	testBatch := &topicreadercommon.PublicBatch{
		Messages: []*topicreadercommon.PublicMessage{
			// Add test messages if needed
		},
	}

	metadata := rawtopiccommon.ServerMessageMetadata{
		Status: rawydb.StatusSuccess,
	}

	worker.SendBatchMessage(metadata, testBatch)

	// Wait for processing to complete instead of sleeping
	xtest.WaitChannelClosed(t, processingDone)
	require.Nil(t, stoppedErr)

	// Verify ReadRequest was sent for flow control
	messages := messageSender.GetMessages()
	require.Len(t, messages, 1)

	readReq, ok := messages[0].(*rawtopicreader.ReadRequest)
	require.True(t, ok)
	require.GreaterOrEqual(t, readReq.BytesSize, 0) // Empty batch results in 0 bytes
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

	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped)

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
	worker.SendRawServerMessage(startReq)

	// Wait for error handling using channel instead of Eventually
	xtest.WaitChannelClosed(t, errorReceived)

	// Verify error propagation through public callback
	require.Equal(t, int64(789), stoppedSessionID.Load())
	errPtr := stoppedErr.Load()
	require.NotNil(t, errPtr)
	require.Contains(t, (*errPtr).Error(), expectedErr.Error())
}

func TestPartitionWorkerInterface_CommitMessageFlow(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := createTestPartitionSession()
	messageSender := newSyncMessageSender()
	mockHandler := NewMockEventHandler(ctrl)

	var stoppedErr error
	onStopped := func(sessionID int64, err error) {
		stoppedErr = err
	}

	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped)

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Create a test commit response
	commitResponse := &rawtopicreader.CommitOffsetResponse{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		PartitionsCommittedOffsets: []rawtopicreader.PartitionCommittedOffset{
			{
				PartitionSessionID: session.StreamPartitionSessionID,
				CommittedOffset:    rawtopiccommon.NewOffset(150),
			},
		},
	}

	// Send commit message
	worker.SendCommitMessage(commitResponse)

	// Give some time for processing
	time.Sleep(10 * time.Millisecond)

	// Verify that committed offset was updated in session
	require.Equal(t, int64(150), session.CommittedOffset().ToInt64())
	require.Nil(t, stoppedErr)
}

// =============================================================================
// IMPLEMENTATION TESTS - Test internal behavior and edge cases
// =============================================================================

func TestPartitionWorkerImpl_QueueClosureHandling(t *testing.T) {
	ctx := xtest.Context(t)

	session := createTestPartitionSession()
	messageSender := newSyncMessageSender()

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

	worker := NewPartitionWorker(789, session, messageSender, nil, onStopped)

	worker.Start(ctx)

	// Close the worker immediately to trigger queue closure
	err := worker.Close(ctx, nil)
	require.NoError(t, err)

	// Wait for error handling using channel instead of Eventually
	xtest.WaitChannelClosed(t, errorReceived)

	// Verify error propagation through public callback
	require.Equal(t, int64(789), stoppedSessionID.Load())
	errPtr := stoppedErr.Load()
	require.NotNil(t, errPtr)
	if *errPtr != nil {
		require.Contains(t, (*errPtr).Error(), "partition messages queue closed")
	}
}

func TestPartitionWorkerImpl_ContextCancellation(t *testing.T) {
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

	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped)

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)

	// Cancel the context to trigger graceful shutdown
	cancel()

	// Wait for error handling using channel instead of Eventually
	xtest.WaitChannelClosed(t, errorReceived)

	// Verify graceful shutdown (nil error)
	require.Equal(t, int64(789), stoppedSessionID.Load())
	errPtr := stoppedErr.Load()
	require.NotNil(t, errPtr)
	require.Nil(t, *errPtr) // Graceful shutdown should have nil error
}

func TestPartitionWorkerImpl_PanicRecovery(t *testing.T) {
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

	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped)

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

	// Send start partition request that will cause panic
	startReq := createTestStartPartitionRequest()
	worker.SendRawServerMessage(startReq)

	// Wait for error handling using channel instead of Eventually
	xtest.WaitChannelClosed(t, errorReceived)

	// Verify panic recovery
	require.Equal(t, int64(789), stoppedSessionID.Load())
	errPtr := stoppedErr.Load()
	require.NotNil(t, errPtr)
	require.Contains(t, (*errPtr).Error(), "partition worker panic")
}

func TestPartitionWorkerImpl_MessageTypeHandling(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := createTestPartitionSession()
	messageSender := newSyncMessageSender()
	mockHandler := NewMockEventHandler(ctrl)

	var stoppedErr error
	onStopped := func(sessionID int64, err error) {
		stoppedErr = err
	}

	worker := NewPartitionWorker(789, session, messageSender, mockHandler, onStopped)

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Send empty unified message (should be ignored)
	worker.SendMessage(unifiedMessage{})

	// Give some time for processing
	time.Sleep(10 * time.Millisecond)

	// Verify no error occurred
	require.Nil(t, stoppedErr)
}
