package conn

import (
	"github.com/google/uuid"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type newTraceIDOpts struct {
	newRandom func() (uuid.UUID, error)
}

func newTraceID(opts ...func(opts *newTraceIDOpts)) (string, error) {
	options := newTraceIDOpts{newRandom: uuid.NewRandom}
	for _, opt := range opts {
		opt(&options)
	}
	uuid, err := options.newRandom()
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}
	return uuid.String(), nil
}
