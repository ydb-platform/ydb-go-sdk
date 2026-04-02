package framework

import (
	"context"
	"strings"
	"unicode"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const (
	operationErrorCategory   = "operation"
	operationTimeoutCategory = "timeout"
)

func classifyError(err error) (category, name string) {
	if err == nil {
		return "", ""
	}

	if ydb.IsOperationErrorTransactionLocksInvalidated(err) {
		return operationErrorCategory, "ydb/aborted/tli"
	}

	if opErr := ydb.OperationError(err); opErr != nil {
		return operationErrorCategory, normalizeErrorName(opErr.Name())
	}

	if trErr := ydb.TransportError(err); trErr != nil {
		return "transport", normalizeErrorName(trErr.Name())
	}

	switch {
	case context.DeadlineExceeded != nil && strings.Contains(err.Error(), context.DeadlineExceeded.Error()):
		return operationTimeoutCategory, "context/deadline_exceeded"
	case context.Canceled != nil && strings.Contains(err.Error(), context.Canceled.Error()):
		return operationTimeoutCategory, "context/canceled"
	}

	return "other", "unknown/" + truncate(normalizeMessage(err.Error()), 64)
}

func normalizeErrorName(name string) string {
	result := strings.Map(func(r rune) rune {
		if unicode.IsUpper(r) {
			return unicode.ToLower(r)
		}
		if r == ' ' {
			return '_'
		}

		return r
	}, name)

	// transport/unavailable -> grpc/unavailable
	result = strings.Replace(result, "transport/", "grpc/", 1)
	// operation/aborted -> ydb/aborted
	result = strings.Replace(result, "operation/", "ydb/", 1)

	return result
}

func normalizeMessage(msg string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(msg) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '/' || r == '_':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('_')
		}
	}

	return b.String()
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	return s[:maxLen]
}
