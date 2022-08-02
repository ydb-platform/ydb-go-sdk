package trace

import (
	"context"
	"io"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Topic specified trace of topic reader client activity.
	// gtrace:gen
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	Topic struct {
		OnPartitionReadStart       func(OnPartitionReadStartInfo)
		OnPartitionReadStop        func(OnPartitionReadStopInfo)
		OnPartitionCommittedNotify func(OnPartitionCommittedInfo)

		OnReaderStreamConnect func(OnReaderStreamConnectStartInfo) func(OnReaderStreamConnectDoneInfo)
		OnReaderStreamClose   func(OnReaderStreamCloseStartInfo) func(OnReaderStreamCloseDoneInfo)
		OnReadStreamInit      func(OnReadStreamInitStartInfo) func(OnReadStreamInitDoneInfo)
		OnReadStreamError     func(OnReadStreamErrorInfo)

		OnReadUnknownGrpcMessage func(OnReadUnknownGrpcMessageInfo)
		OnReadStreamRawReceived  func(OnReadStreamRawReceivedInfo)
		OnReadStreamRawSent      func(OnReadStreamRawSentInfo)
		OnReadStreamUpdateToken  func(OnReadStreamUpdateTokenInfo)
	}

	// OnPartitionReadStartInfo
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnPartitionReadStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		ReadOffset         *int64
		CommitOffset       *int64
	}

	// OnPartitionReadStopInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnPartitionReadStopInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
		Graceful           bool
	}

	// OnPartitionCommittedInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnPartitionCommittedInfo struct {
		ReaderConnectionID string
		Topic              string
		PartitionID        int64
		CommittedOffset    int64
	}

	// OnReadStreamErrorInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamErrorInfo struct {
		BaseContext        context.Context
		ReaderConnectionID string
		Error              error
	}

	// OnReadUnknownGrpcMessageInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadUnknownGrpcMessageInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		ServerMessage      readStreamServerMessageDebugInfo // may be nil
		Error              error
	}

	// OnReaderStreamConnectStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamConnectStartInfo struct{}

	// OnReaderStreamConnectDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamConnectDoneInfo struct {
		Error error
	}

	OnReaderStreamCloseStartInfo struct {
		ReaderConnectionID string
		CloseReason        error
	}

	OnReaderStreamCloseDoneInfo struct {
		ReaderConnectionID string
		CloseReason        error
		CloseError         error
	}

	// OnReadStreamInitStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamInitStartInfo struct {
		PreInitReaderConnectionID string
	}

	// OnReadStreamInitDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamInitDoneInfo struct {
		PreInitReaderConnectionID string
		ReaderConnectionID        string
		Error                     error
	}

	readStreamServerMessageDebugInfo interface {
		Type() string
		JSONData() io.Reader
		IsReadStreamServerMessageDebugInfo()
	}

	// OnReadStreamRawReceivedInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamRawReceivedInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		ServerMessage      readStreamServerMessageDebugInfo
		Error              error
	}

	readStreamClientMessageDebugInfo interface {
		Type() string
		JSONData() io.Reader
		IsReadStreamClientMessageDebugInfo()
	}

	// OnReadStreamRawSentInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamRawSentInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		ClientMessage      readStreamClientMessageDebugInfo
		Error              error
	}

	// OnReadStreamUpdateTokenInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamUpdateTokenInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		TokenLen           int
		Error              error
	}
)
