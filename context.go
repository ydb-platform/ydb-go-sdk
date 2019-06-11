package ydb

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/duration"

	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Operations"
	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

type (
	ctxOpTimeoutKey     struct{}
	ctxOpCancelAfterKey struct{}
	ctxOpModeKey        struct{}
)

// ContextDeadlineMapping describes how context.Context's deadline value is
// used for YDB operation options.
type ContextDeadlineMapping uint

const (
	// ContextDeadlineNoMapping disables mapping of context's deadline value.
	ContextDeadlineNoMapping ContextDeadlineMapping = iota

	// ContextDeadlineOperationTimeout uses context's deadline value as
	// operation timeout.
	ContextDeadlineOperationTimeout

	// ContextDeadlineOperationCancelAfter uses context's deadline value as
	// operation cancelation timeout.
	ContextDeadlineOperationCancelAfter
)

// WithOperationTimeout returns a copy of parent in which YDB operation timeout
// parameter is set to d. If parent timeout is smaller than d, parent context
// is returned.
func WithOperationTimeout(parent context.Context, d time.Duration) context.Context {
	if cur, ok := ContextOperationTimeout(parent); ok && d >= cur {
		// The current timeout is already smaller than the new one.
		return parent
	}
	return context.WithValue(parent, ctxOpTimeoutKey{}, d)
}

// ContextOperationTimeout returns the timeout within given context after which
// YDB should try to cancel operation and return result regardless of the
// cancelation.
func ContextOperationTimeout(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOpTimeoutKey{}).(time.Duration)
	return
}

// WithOperationCancelAfter returns a copy of parent in which YDB operation
// cancel after parameter is set to d. If parent cancelation timeout is smaller
// than d, parent context is returned.
func WithOperationCancelAfter(parent context.Context, d time.Duration) context.Context {
	if cur, ok := ContextOperationCancelAfter(parent); ok && d >= cur {
		// The current cancelation timeout is already smaller than the new one.
		return parent
	}
	return context.WithValue(parent, ctxOpCancelAfterKey{}, d)
}

// ContextOperationTimeout returns the timeout within given context after which
// YDB should try to cancel operation and return result regardless of the
// cancelation.
func ContextOperationCancelAfter(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOpCancelAfterKey{}).(time.Duration)
	return
}

// WithOperationMode returns a copy of parent in which YDB operation mode
// parameter is set to m. If parent mode is set and is not equal to m,
// WithOperationMode will panic.
func WithOperationMode(parent context.Context, m OperationMode) context.Context {
	if cur, ok := ContextOperationMode(parent); ok {
		if cur != m {
			panic(fmt.Sprintf(
				"ydb: context already has different operation mode: %v; %v given",
				cur, m,
			))
		}
		return parent
	}
	return context.WithValue(parent, ctxOpModeKey{}, m)
}

// ContextOperationMode returns the mode of YDB operation within given context.
func ContextOperationMode(ctx context.Context) (m OperationMode, ok bool) {
	m, ok = ctx.Value(ctxOpModeKey{}).(OperationMode)
	return
}

type OperationMode uint

const (
	OperationModeUnknown OperationMode = iota
	OperationModeSync
	OperationModeAsync
)

func (m OperationMode) String() string {
	switch m {
	case OperationModeSync:
		return "sync"
	case OperationModeAsync:
		return "async"
	default:
		return "unknown"
	}
}
func (m OperationMode) toYDB() Ydb_Operations.OperationParams_OperationMode {
	switch m {
	case OperationModeSync:
		return Ydb_Operations.OperationParams_SYNC
	case OperationModeAsync:
		return Ydb_Operations.OperationParams_ASYNC
	default:
		return Ydb_Operations.OperationParams_OPERATION_MODE_UNSPECIFIED
	}
}

func setOperationParams(
	ctx context.Context, dm ContextDeadlineMapping,
	req interface{},
) {
	x, ok := req.(interface {
		SetOperationParams(*Ydb_Operations.OperationParams)
	})
	if !ok {
		return
	}
	var (
		timeout     *duration.Duration
		cancelAfter *duration.Duration
		mode        Ydb_Operations.OperationParams_OperationMode
	)

	d, hasT := ContextOperationTimeout(ctx)
	if hasT {
		timeout = timeoutParam(d)
	}

	d, hasC := ContextOperationCancelAfter(ctx)
	if hasC {
		cancelAfter = timeoutParam(d)
	}

	d, hasD := contextUntilDeadline(ctx)
	if !hasT && hasD && dm == ContextDeadlineOperationTimeout {
		timeout = timeoutParam(d)
	}
	if !hasC && hasD && dm == ContextDeadlineOperationCancelAfter {
		cancelAfter = timeoutParam(d)
	}

	if m, hasM := ContextOperationMode(ctx); hasM {
		mode = m.toYDB()
	}

	if mode == 0 && timeout == nil && cancelAfter == nil {
		// Avoid OperationParams allocation.
		return
	}

	x.SetOperationParams(&Ydb_Operations.OperationParams{
		OperationMode:    mode,
		OperationTimeout: timeout,
		CancelAfter:      cancelAfter,
	})
}

func timeoutParam(d time.Duration) *duration.Duration {
	if d > 0 {
		return ptypes.DurationProto(d)
	}
	return nil
}

func contextUntilDeadline(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if ok {
		return timeutil.Until(deadline), true
	}
	return 0, false
}
