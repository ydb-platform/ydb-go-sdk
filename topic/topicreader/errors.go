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

// ErrConcurrencyCall return if method on reader called in concurrency
// client side must check error with errors.Is
var ErrConcurrencyCall = xerrors.Wrap(errors.New("ydb: concurrency call denied"))

// ErrConcurrencyCallRead return if read method (ReadMessage, ReadMessagesBatch, etc.) called concurrently.
// client side must check error with errors.Is
var ErrConcurrencyCallRead = fmt.Errorf(
	"%w; possibly, you have read operations from concurrent goroutines", ErrConcurrencyCall,
)

// ErrConcurrencyCallCommit return if Commit called concurrently.
// client side must check error with errors.Is
var ErrConcurrencyCallCommit = fmt.Errorf(
	"%w; possibly, you have commit operations from concurrent goroutines", ErrConcurrencyCall,
)

// ErrCommitToExpiredSession it is not fatal error and reader can continue work
// client side must check error with errors.Is
var ErrCommitToExpiredSession = topicreadercommon.ErrPublicCommitSessionToExpiredSession
