package topiclistenerinternal

import (
	"context"
	"errors"
	"strings"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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

// Implement CommitHandler interface for tests
func (m *mockMessageSender) sendCommit(b *topicreadercommon.PublicBatch) error {
	// For tests, just record the commit as a message
	m.SendRaw(&rawtopicreader.ReadRequest{BytesSize: -1}) // Use negative size to indicate commit

	return nil
}

func (m *mockMessageSender) getSyncCommitter() SyncCommitter {
	return &mockSyncCommitter{}
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

// mockSyncCommitter provides a mock implementation of SyncCommitter for tests
type mockSyncCommitter struct{}

func (m *mockSyncCommitter) Commit(ctx context.Context, commitRange topicreadercommon.CommitRange) error {
	return nil
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
	return &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: rawtopicreader.PartitionSessionID(456),
		Graceful:           graceful,
		CommittedOffset:    rawtopiccommon.NewOffset(200),
	}
}

func createTestBatch() *topicreadercommon.PublicBatch {
	// Create a minimal test batch with correct PublicMessage fields
	return &topicreadercommon.PublicBatch{
		Messages: []*topicreadercommon.PublicMessage{
			{
				// Use the correct unexported field name for test purposes
				UncompressedSize: 12,
				Offset:           100, // Use int64 directly
			},
		},
	}
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
	onStopped := func(sessionID rawtopicreader.PartitionSessionID, err error) {
		stoppedErr = err
	}

	worker := NewPartitionWorker(
		123,
		session,
		messageSender,
		mockHandler,
		onStopped,
		&trace.Topic{},
		"test-listener",
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
	worker.AddRawServerMessage(startReq)

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
		onStopped := func(sessionID rawtopicreader.PartitionSessionID, err error) {
			stoppedErr = err
		}

		worker := NewPartitionWorker(
			123,
			session,
			messageSender,
			mockHandler,
			onStopped,
			&trace.Topic{},
			"test-listener",
		)

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
		worker.AddRawServerMessage(stopReq)

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
		onStopped := func(sessionID rawtopicreader.PartitionSessionID, err error) {
			stoppedErr = err
		}

		worker := NewPartitionWorker(
			123,
			session,
			messageSender,
			mockHandler,
			onStopped,
			&trace.Topic{},
			"test-listener",
		)

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
		worker.AddRawServerMessage(stopReq)

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
	onStopped := func(sessionID rawtopicreader.PartitionSessionID, err error) {
		stoppedErr = err
	}

	worker := NewPartitionWorker(
		123,
		session,
		messageSender,
		mockHandler,
		onStopped,
		&trace.Topic{},
		"test-listener",
	)

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

	worker.AddMessagesBatch(metadata, testBatch)

	// Wait for processing to complete
	xtest.WaitChannelClosed(t, processingDone)

	// Wait for the ReadRequest to be sent (small additional wait for async SendRaw)
	err := messageSender.waitForMessage(ctx)
	require.NoError(t, err)

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
	errorReceived := make(empty.Chan, 1)
	onStopped := func(sessionID rawtopicreader.PartitionSessionID, err error) {
		stoppedSessionID.Store(sessionID.ToInt64())
		stoppedErr.Store(&err)
		// Signal that error was received
		select {
		case errorReceived <- empty.Struct{}:
		default:
		}
	}

	worker := NewPartitionWorker(
		123,
		session,
		messageSender,
		mockHandler,
		onStopped,
		&trace.Topic{},
		"test-listener",
	)

	// Set up mock to return error
	mockHandler.EXPECT().
		OnReadMessages(gomock.Any(), gomock.Any()).
		Return(errors.New("user handler error"))

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Create a test batch
	batch := createTestBatch()
	metadata := rawtopiccommon.ServerMessageMetadata{
		Status: rawydb.StatusSuccess,
	}

	// Send batch message that will cause error
	worker.AddMessagesBatch(metadata, batch)

	// Wait for error handling using channel instead of Eventually
	xtest.WaitChannelClosed(t, errorReceived)

	// Verify error contains user handler error using atomic access
	require.Equal(t, int64(123), stoppedSessionID.Load())
	errPtr := stoppedErr.Load()
	require.NotNil(t, errPtr)
	require.Contains(t, (*errPtr).Error(), "user handler error")
}

// Note: CommitMessage processing has been moved to streamListener
// and is no longer handled by PartitionWorker

// =============================================================================
// IMPLEMENTATION TESTS - Test internal behavior and edge cases
// =============================================================================

func TestPartitionWorkerImpl_QueueClosureHandling(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := createTestPartitionSession()
	messageSender := newSyncMessageSender()
	mockHandler := NewMockEventHandler(ctrl)

	var stoppedSessionID atomic.Int64
	var stoppedErr atomic.Pointer[error]
	errorReceived := make(empty.Chan, 1)
	onStopped := func(sessionID rawtopicreader.PartitionSessionID, err error) {
		stoppedSessionID.Store(sessionID.ToInt64())
		stoppedErr.Store(&err)
		// Signal that error was received
		select {
		case errorReceived <- empty.Struct{}:
		default:
		}
	}

	worker := NewPartitionWorker(
		123,
		session,
		messageSender,
		mockHandler,
		onStopped,
		&trace.Topic{},
		"test-listener",
	)

	worker.Start(ctx)

	// Close the worker immediately to trigger queue closure
	err := worker.Close(ctx, nil)
	require.NoError(t, err)

	// Wait for error handling using channel instead of Eventually
	xtest.WaitChannelClosed(t, errorReceived)

	// Verify error propagation through public callback
	require.Equal(t, int64(123), stoppedSessionID.Load())
	errPtr := stoppedErr.Load()
	require.NotNil(t, errPtr)
	if *errPtr != nil {
		// When Close() is called, there's a race between queue closure and context cancellation
		// Both are valid shutdown reasons, so accept either one
		errorMsg := (*errPtr).Error()
		isQueueClosed := strings.Contains(errorMsg, "partition worker message queue closed")
		isContextCanceled := strings.Contains(errorMsg, "partition worker message queue context error") &&
			strings.Contains(errorMsg, "context canceled")
		require.True(t, isQueueClosed || isContextCanceled,
			"Expected either queue closure or context cancellation error, got: %s", errorMsg)
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
	errorReceived := make(empty.Chan, 1)
	onStopped := func(sessionID rawtopicreader.PartitionSessionID, err error) {
		stoppedSessionID.Store(sessionID.ToInt64())
		stoppedErr.Store(&err)
		// Signal that error was received
		select {
		case errorReceived <- empty.Struct{}:
		default:
		}
	}

	worker := NewPartitionWorker(
		123,
		session,
		messageSender,
		mockHandler,
		onStopped,
		&trace.Topic{},
		"test-listener",
	)

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)

	// Cancel the context to trigger graceful shutdown
	cancel()

	// Wait for error handling using channel instead of Eventually
	xtest.WaitChannelClosed(t, errorReceived)

	// Verify graceful shutdown (proper reason provided)
	require.Equal(t, int64(123), stoppedSessionID.Load())
	errPtr := stoppedErr.Load()
	require.NotNil(t, errPtr)
	require.NotNil(t, *errPtr) // Graceful shutdown should have meaningful reason
	require.Contains(t, (*errPtr).Error(), "graceful shutdown PartitionWorker")
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
	errorReceived := make(empty.Chan, 1)
	onStopped := func(sessionID rawtopicreader.PartitionSessionID, err error) {
		stoppedSessionID.Store(sessionID.ToInt64())
		stoppedErr.Store(&err)
		// Signal that error was received
		select {
		case errorReceived <- empty.Struct{}:
		default:
		}
	}

	worker := NewPartitionWorker(
		123,
		session,
		messageSender,
		mockHandler,
		onStopped,
		&trace.Topic{},
		"test-listener",
	)

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
	worker.AddRawServerMessage(startReq)

	// Wait for error handling using channel instead of Eventually
	xtest.WaitChannelClosed(t, errorReceived)

	// Verify panic recovery
	require.Equal(t, int64(123), stoppedSessionID.Load())
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
	onStopped := func(sessionID rawtopicreader.PartitionSessionID, err error) {
		stoppedErr = err
	}

	worker := NewPartitionWorker(
		123,
		session,
		messageSender,
		mockHandler,
		onStopped,
		&trace.Topic{},
		"test-listener",
	)

	worker.Start(ctx)
	defer func() {
		err := worker.Close(ctx, nil)
		require.NoError(t, err)
	}()

	// Send empty unified message (should be ignored)
	worker.AddUnifiedMessage(unifiedMessage{})

	// Give some time for processing
	time.Sleep(10 * time.Millisecond)

	// Verify no error occurred
	require.Nil(t, stoppedErr)
}
