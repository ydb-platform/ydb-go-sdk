package xerrors

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorf(t *testing.T) {
	for _, tt := range []struct {
		err *multiErrorf
		iss []error
		ass []interface{}
		s   string
	}{
		{
			err: MultiErrorf("simple"),
			s:   "simple",
		},
		{
			err: MultiErrorf("format int(%d) and string(%q)", 100500, "azaza"),
			s:   "format int(100500) and string(\"azaza\")",
		},
		{
			err: MultiErrorf("format int(%d) and string(%q) (error '%w')", 100500, "azaza", context.Canceled),
			iss: []error{context.Canceled},
			ass: nil,
			s:   "format int(100500) and string(\"azaza\") (error 'context canceled')",
		},
		{
			err: MultiErrorf("format int(%d), error '%w' and string(%q)", 100500, context.Canceled, "azaza"),
			iss: []error{context.Canceled},
			ass: nil,
			s:   "format int(100500), error 'context canceled' and string(\"azaza\")",
		},
		{
			err: MultiErrorf("format int(%d), error1 '%w', error2 '%w' and string(%q)", 100500, context.Canceled, context.DeadlineExceeded, "azaza"),
			iss: []error{context.Canceled, context.DeadlineExceeded},
			ass: nil,
			s:   "format int(100500), error1 'context canceled', error2 'context deadline exceeded' and string(\"azaza\")",
		},
		{
			err: MultiErrorf("format int(%d), error1 '%w', error2 '%w' and string(%q)", 100500, Wrap(context.Canceled), context.DeadlineExceeded, "azaza"),
			iss: []error{context.Canceled, context.DeadlineExceeded},
			ass: []interface{}{func() interface{} { var i isYdbError; return &i }()},
			s:   "format int(100500), error1 'context canceled', error2 'context deadline exceeded' and string(\"azaza\")",
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
