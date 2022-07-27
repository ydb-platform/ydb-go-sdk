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
		OnPartitionReadStop        func(info OnPartitionReadStopInfo)
		OnPartitionCommittedNotify func(OnPartitionCommittedInfo)

		OnReadStreamOpen  func(OnReadStreamOpenInfo)
		OnReadStreamInit  func(OnReadStreamInitInfo)
		OnReadStreamError func(OnReadStreamErrorInfo)

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

	// OnReadStreamOpenInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamOpenInfo struct {
		BaseContext context.Context
		Error       error
	}

	// OnReadStreamInitInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamInitInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		Error              error
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
