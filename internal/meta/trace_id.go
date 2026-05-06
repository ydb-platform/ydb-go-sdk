package meta

import (
	"context"
	"encoding/binary"
	rand2 "math/rand"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type newTraceIDOpts struct {
	newRandom func() (uuid.UUID, error)
}

var (
	counter atomic.Uint64
	delta   = func() uint64 {
		delta := rand2.NewSource(time.Now().Unix()).Int63()
		if delta%2 == 0 {
			return uint64(delta + 1)
		}

		return uint64(delta)
	}()
)

func fastUUID() (uuid uuid.UUID, _ error) { //nolint:unparam
	binary.LittleEndian.PutUint64(uuid[:8], counter.Load())
	binary.LittleEndian.PutUint64(uuid[8:], counter.Add(delta))

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
