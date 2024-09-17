package xerrors

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	for _, tt := range []struct {
		join error
		iss  []error
		ass  []interface{}
		s    string
	}{
		{
			join: Join(context.Canceled),
			iss:  []error{context.Canceled},
			ass:  nil,
			s:    "context canceled",
		},
		{
			join: Join(context.Canceled, context.DeadlineExceeded, Operation()),
			iss:  []error{context.Canceled, context.DeadlineExceeded},
			ass: []interface{}{func() interface{} {
				var i isYdbError

				return &i
			}()},
			s: "[\"context canceled\",\"context deadline exceeded\",\"operation/STATUS_CODE_UNSPECIFIED (code = 0)\"]",
		},
		{
			join: Join(context.Canceled, context.DeadlineExceeded, nil),
			iss:  []error{context.Canceled, context.DeadlineExceeded},
			s:    "[\"context canceled\",\"context deadline exceeded\"]",
		},
		{
			join: Join(nil, context.DeadlineExceeded, nil),
			iss:  []error{context.DeadlineExceeded},
			s:    "context deadline exceeded",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.s, tt.join.Error())
			if len(tt.iss) > 0 {
				require.True(t, Is(tt.join, tt.iss...))
			}
			if len(tt.ass) > 0 {
				require.True(t, As(tt.join, tt.ass...))
			}
		})
	}
}

func TestUnwrapJoined(t *testing.T) {
	err1 := context.Canceled
	err2 := context.DeadlineExceeded

	joined := Join(err1, err2)

	unwrappable := joined.(interface{ Unwrap() []error })
	inners := unwrappable.Unwrap()
	assert.Contains(t, inners, err1)
	assert.Contains(t, inners, err2)
}
