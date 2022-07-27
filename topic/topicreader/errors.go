package topicreader

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// ErrUnexpectedCodec will return if topicreader receive message with unknown codec.
// client side must check error with errors.Is
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
var ErrUnexpectedCodec = topicreaderinternal.PublicErrUnexpectedCodec

// ErrConcurrencyCall return if method on reader called in concurrency
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
var ErrConcurrencyCall = xerrors.Wrap(errors.New("concurrency call"))
