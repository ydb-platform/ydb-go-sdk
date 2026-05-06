package meta

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"sync/atomic"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type newTraceIDOpts struct {
	newRandom func() (uuid.UUID, error)
}

var (
	counter atomic.Uint64
	seed    = func() (seed [16]byte) {
		rand.Read(seed[:])

		return seed
	}()
)

func fastUUID() (uuid uuid.UUID, _ error) { //nolint:unparam
	x := counter.Add(1)
	uuid = seed
	binary.LittleEndian.PutUint64(uuid[:8], x)

	return uuid, nil
}

func TraceID(ctx context.Context, opts ...func(opts *newTraceIDOpts)) (context.Context, string, error) {
	if id, has := traceID(ctx); has {
		return ctx, id, nil
	}

	options := newTraceIDOpts{
		newRandom: fastUUID,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	uuid, err := options.newRandom()
	if err != nil {
		return ctx, "", xerrors.WithStackTrace(err)
	}

	id := uuid.String()

	return metadata.AppendToOutgoingContext(ctx, HeaderTraceID, id), id, nil
}
