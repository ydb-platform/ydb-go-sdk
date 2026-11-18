package topicreader

import (
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// ErrUnexpectedCodec will return if topicreader receive message with unknown codec.
// client side must check error with errors.Is
var ErrUnexpectedCodec = topicreadercommon.ErrPublicUnexpectedCodec

// ErrConcurrencyCall is returned when methods on [Reader] are called concurrently in a disallowed way.
// See [Reader] for the concurrency matrix (which pairs of methods may run concurrently).
// Client code must check the error with errors.Is.
var ErrConcurrencyCall = xerrors.Wrap(errors.New("ydb: concurrency call denied"))

var errConcurrencyCallRead = fmt.Errorf(
	"%w; possibly, you have read operations from concurrent goroutines", ErrConcurrencyCall,
)

var errConcurrencyCallCommit = fmt.Errorf(
	"%w; possibly, you have commit operations from concurrent goroutines", ErrConcurrencyCall,
)

// ErrCommitToExpiredSession reports an expired session during a commit.
// It can be returned by [Reader.Commit] or by the topic client's CommitOffset method.
// A [Reader] can continue working after [Reader.Commit] returns this error.
// Use [errors.Is] to check for it.
var ErrCommitToExpiredSession = topicreadercommon.ErrPublicCommitSessionToExpiredSession
