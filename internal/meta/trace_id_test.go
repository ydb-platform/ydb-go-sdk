package meta

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestTraceID(t *testing.T) {
	t.Run("TraceID from rand", func(t *testing.T) {
		ctx, id, err := TraceID(
			context.Background(),
			func(opts *newTraceIDOpts) {
				opts.newRandom = func() (uuid.UUID, error) {
					return uuid.UUID{}, nil
				}
			},
		)
		require.NoError(t, err)
		require.Equal(t, "00000000-0000-0000-0000-000000000000", id)
		md, has := metadata.FromOutgoingContext(ctx)
		require.True(t, has)
		require.Len(t, md[HeaderTraceID], 1)
		require.Equal(t, id, md[HeaderTraceID][0])
	})
	t.Run("TraceID from rand failed", func(t *testing.T) {
		_, _, err := TraceID(
			context.Background(),
			func(opts *newTraceIDOpts) {
				opts.newRandom = func() (uuid.UUID, error) {
					return uuid.UUID{}, errors.New("")
				}
			},
		)
		require.Error(t, err)
	})
	t.Run("TraceID from outgoing metadata", func(t *testing.T) {
		ctx, id, err := TraceID(
			WithTraceID(context.Background(), "{test}"),
			func(opts *newTraceIDOpts) {
				opts.newRandom = func() (uuid.UUID, error) {
					return uuid.UUID{}, errors.New("")
				}
			},
		)
		require.NoError(t, err)
		require.Equal(t, "{test}", id)
		md, has := metadata.FromOutgoingContext(ctx)
		require.True(t, has)
		require.Len(t, md[HeaderTraceID], 1)
		require.Equal(t, id, md[HeaderTraceID][0])
	})
}

// BenchmarkNewRandom/fastUUID-12         	594720858	         1.865 ns/op	       0 B/op	       0 allocs/op
// BenchmarkNewRandom/uuid.NewRandom()-12 	 5434495	       229.1 ns/op	      16 B/op	       1 allocs/op
// BenchmarkNewRandom/uuid.NewUUID()-12   	30184470	        39.31 ns/op	       0 B/op	       0 allocs/op
func BenchmarkNewRandom(b *testing.B) {
	b.Run("fastUUID", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = fastUUID()
		}
	})
	b.Run("uuid.NewRandom()", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = uuid.NewRandom()
		}
	})
	b.Run("uuid.NewUUID()", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = uuid.NewUUID()
		}
	})
}
