package wait

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// waitBackoff is a helper function that waits for i-th Backoff b or ctx
// expiration.
// It returns non-nil error if and only if deadline expiration branch wins.
func waitBackoff(ctx context.Context, b backoff.Backoff, i int) error {
	select {
	case <-b.Wait(i):
		return nil
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			return xerrors.WithStackTrace(err)
		}
		return nil
	}
}

func Wait(ctx context.Context, fastBackoff backoff.Backoff, slowBackoff backoff.Backoff, t backoff.Type, i int) error {
	var b backoff.Backoff
	switch t {
	case backoff.TypeNoBackoff:
		return nil
	case backoff.TypeFast:
		b = fastBackoff
	case backoff.TypeSlow:
		b = slowBackoff
	}
	return waitBackoff(ctx, b, i)
}
