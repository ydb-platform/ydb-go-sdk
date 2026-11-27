package topicreadercommon

import (
	"context"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	unknownSessionErrorStatusCode = "unknown"
	transportSessionErrorType     = "transport_error"
	ydbSessionErrorType           = "ydb_error"
	unknownSessionErrorType       = "unknown"
)

// SessionErrorClassification contains the labels derived from an
// error reported by a topic reader session.
type SessionErrorClassification struct {
	StatusCode string
	ErrorType  string
}

// ClassifySessionError maps a transport or YDB operation error to the labels
// used by the topic reader session error trace event.
func ClassifySessionError(err error) SessionErrorClassification {
	classification := SessionErrorClassification{
		StatusCode: unknownSessionErrorStatusCode,
		ErrorType:  unknownSessionErrorType,
	}
	if err == nil {
		return classification
	}

	if xerrors.IsTransportError(err) {
		classification.ErrorType = transportSessionErrorType
		classification.StatusCode = grpcStatus.Code(err).String()

		return classification
	}

	if operationErr := xerrors.OperationError(err); operationErr != nil {
		classification.ErrorType = ydbSessionErrorType
		classification.StatusCode = ydbStatusCodeName(operationErr.Code())

		return classification
	}

	return classification
}

// TraceReaderSessionError emits one session error event for a caller
// that has already made the retry or stop decision.
func TraceReaderSessionError(
	ctx context.Context,
	tracer *trace.Topic,
	readerInfo ReaderInfo,
	retryDecision string,
	err error,
) {
	if tracer == nil || tracer.OnReaderSessionError == nil || err == nil {
		return
	}

	classification := ClassifySessionError(err)
	waitForSessionTrace(ctx, func(traceCtx context.Context) {
		gtrace.TopicOnReaderSessionError(
			tracer,
			&traceCtx,
			readerInfo,
			retryDecision,
			classification.StatusCode,
			classification.ErrorType,
			err,
		)
	})
}

func waitForSessionTrace(ctx context.Context, callback func(context.Context)) {
	callbackDone := make(chan any, 1)
	go func() {
		defer func() { callbackDone <- recover() }()
		callback(ctx)
	}()

	// Close cancels the worker before joining it. Stop waiting on cancellation
	// so a callback that calls Close does not wait on itself. After that point
	// there is no caller to re-panic on; its buffered result is discarded.
	select {
	case p := <-callbackDone:
		if p != nil {
			panic(p)
		}
	case <-ctx.Done():
	}
}

func ydbStatusCodeName(code int32) string {
	name, ok := Ydb.StatusIds_StatusCode_name[code]
	if ok && name != "STATUS_CODE_UNSPECIFIED" {
		return name
	}
	if code == int32(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED) {
		return unknownSessionErrorStatusCode
	}

	return strconv.FormatInt(int64(code), 10)
}
