package log

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestColoring(t *testing.T) {
	zeroClock := clockwork.NewFakeClockAt(
		time.Date(1984, 4, 4, 0, 0, 0, 0, time.UTC),
	)
	for _, tt := range []struct {
		l   *defaultLogger
		msg string
		exp string
	}{
		{
			l: &defaultLogger{
				coloring: true,
				clock:    zeroClock,
			},
			msg: "test",
			exp: "\u001B[31m1984-04-04 00:00:00.000 \u001B[0m\u001B[101mERROR\u001B[0m\u001B[31m 'test.scope' => message\u001B[0m", //nolint:lll
		},
		{
			l: &defaultLogger{
				coloring: false,
				clock:    zeroClock,
			},
			msg: "test",
			exp: "1984-04-04 00:00:00.000 ERROR 'test.scope' => message",
		},
	} {
		t.Run("", func(t *testing.T) {
			act := tt.l.format([]string{"test", "scope"}, "message", ERROR)
			require.Equal(t, tt.exp, act)
		})
	}
}
