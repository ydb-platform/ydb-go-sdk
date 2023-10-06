package wait

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// waitBackoff is a helper function that waits for i-th Backoff b or ctx
// expiration.
// It returns non-nil error if and only if deadline expiration branch wins.
func waitBackoff(ctx context.Context, b backoff.Backoff, i int) error {
	t := time.NewTimer(b.Delay(i))
	defer t.Stop()

	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			return xerrors.WithStackTrace(err)
		}
		return nil
	}
}

func Wait(ctx context.Context, fastBackoff, slowBackoff backoff.Backoff, t backoff.Type, i int) error {
	var b backoff.Backoff
	switch t {
	case backoff.TypeNoBackoff:
		if err := ctx.Err(); err != nil {
			return xerrors.WithStackTrace(err)
		}
		return nil
	case backoff.TypeFast:
		if fastBackoff == nil {
			fastBackoff = backoff.Fast
		}
		b = fastBackoff
	case backoff.TypeSlow:
		if slowBackoff == nil {
			slowBackoff = backoff.Slow
		}
		b = slowBackoff
	}
	return waitBackoff(ctx, b, i)
}
