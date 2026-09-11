package topicreadercommon

import (
	"context"
	"errors"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
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

	var statusErr *rawtopiccommon.StatusCodeError
	if errors.As(err, &statusErr) {
		classification.ErrorType = ydbSessionErrorType
		classification.StatusCode = ydbStatusCodeName(int32(statusErr.Status))

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
	gtrace.TopicOnReaderSessionError(
		tracer,
		&ctx,
		readerInfo.Endpoint,
		readerInfo.Database,
		readerInfo.Consumer,
		readerInfo.ReaderName,
		readerInfo.Listener,
		retryDecision,
		classification.StatusCode,
		classification.ErrorType,
		err,
	)
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
