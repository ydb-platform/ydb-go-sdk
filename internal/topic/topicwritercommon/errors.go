package topicwritercommon

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errNoAllowedCodecs  = xerrors.Wrap(errors.New("ydb: no allowed codecs for write to topic"))
	errNoRawContent     = xerrors.Wrap(errors.New("ydb: internal state error - no raw message content"))
	ErrNonZeroCreatedAt = xerrors.Wrap(errors.New(
		"ydb: non zero Message.CreatedAt and set auto fill created at option",
	))
)
