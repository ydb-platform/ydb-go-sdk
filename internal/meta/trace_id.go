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

var lo, hi = func() (uint64, uint64) {
	var buffer [16]byte
	_, _ = rand.Read(buffer[:])

	return binary.LittleEndian.Uint64(buffer[:8]), binary.LittleEndian.Uint64(buffer[8:])
}()

func fastUUID() (uuid uuid.UUID, _ error) {
	binary.LittleEndian.PutUint64(uuid[:8], lo)
	binary.LittleEndian.PutUint64(uuid[8:], atomic.AddUint64(&hi, 1))
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant RFC 4122

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
