package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestDelay(t *testing.T) {
	for _, tt := range []struct {
		name string
		act  time.Duration
		exp  time.Duration
	}{
		{
			name: xtest.CurrentFileLine(),
			act:  Delay(TypeNoBackoff, 0),
			exp:  0,
		},
		{
			name: xtest.CurrentFileLine(),
			act:  Delay(TypeNoBackoff, 1),
			exp:  0,
		},
		{
			name: xtest.CurrentFileLine(),
			act:  Delay(TypeNoBackoff, 2),
			exp:  0,
		},
		{
			name: xtest.CurrentFileLine(),
			act: Delay(TypeFast, 0, WithFastBackoff(New(
				WithSlotDuration(fastSlot),
				WithCeiling(6),
				WithJitterLimit(1),
			))),
			exp: 5 * time.Millisecond,
		},
		{
			name: xtest.CurrentFileLine(),
			act: Delay(TypeFast, 1, WithFastBackoff(New(
				WithSlotDuration(fastSlot),
				WithCeiling(6),
				WithJitterLimit(1),
			))),
			exp: 10 * time.Millisecond,
		},
		{
			name: xtest.CurrentFileLine(),
			act: Delay(TypeFast, 3, WithFastBackoff(New(
				WithSlotDuration(fastSlot),
				WithCeiling(6),
				WithJitterLimit(1),
			))),
			exp: 40 * time.Millisecond,
		},
		{
			name: xtest.CurrentFileLine(),
			act: Delay(TypeSlow, 0, WithSlowBackoff(New(
				WithSlotDuration(slowSlot),
				WithCeiling(6),
				WithJitterLimit(1),
			))),
			exp: time.Second,
		},
		{
			name: xtest.CurrentFileLine(),
			act: Delay(TypeSlow, 1, WithSlowBackoff(New(
				WithSlotDuration(slowSlot),
				WithCeiling(6),
				WithJitterLimit(1),
			))),
			exp: 2 * time.Second,
		},
		{
			name: xtest.CurrentFileLine(),
			act: Delay(TypeSlow, 3, WithSlowBackoff(New(
				WithSlotDuration(slowSlot),
				WithCeiling(6),
				WithJitterLimit(1),
			))),
			exp: 8 * time.Second,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.exp, tt.act)
		})
	}
}
