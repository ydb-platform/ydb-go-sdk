package xerrors

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	for _, tt := range []struct {
		err joinError
		iss []error
		ass []interface{}
		s   string
	}{
		{
			err: Join(context.Canceled),
			iss: []error{context.Canceled},
			ass: nil,
			s:   "[\"context canceled\"]",
		},
		{
			err: Join(context.Canceled, context.DeadlineExceeded, Operation()),
			iss: []error{context.Canceled, context.DeadlineExceeded},
			ass: []interface{}{func() interface{} {
				var i isYdbError

				return &i
			}()},
			s: "[\"context canceled\",\"context deadline exceeded\",\"operation/STATUS_CODE_UNSPECIFIED (code = 0)\"]",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.s, tt.err.Error())
			if len(tt.iss) > 0 {
				require.True(t, Is(tt.err, tt.iss...))
			}
			if len(tt.ass) > 0 {
				require.True(t, As(tt.err, tt.ass...))
			}
		})
	}
}

func TestUnwrapJoined(t *testing.T) {
	err1 := context.Canceled
	err2 := context.DeadlineExceeded

	var joined error = Join(err1, err2)

	unwrappable, ok := joined.(interface{ Unwrap() []error }) //nolint:errorlint
	if !ok {
		panic(fmt.Sprintf("unsupported type conversion from %T to string", unwrappable))
	}
	inners := unwrappable.Unwrap()
	assert.Contains(t, inners, err1)
	assert.Contains(t, inners, err2)
}
