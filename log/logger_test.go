package log

import (
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestColoring(t *testing.T) {
	for _, tt := range []struct {
		l   *simpleLogger
		msg string
		exp string
	}{
		{
			l: &simpleLogger{
				namespaceMaxLen: 26,
				coloring:        true,
				clock:           clockwork.NewFakeClock(),
			},
			msg: "test",
			exp: "\u001B[31m1984-04-04 00:00:00.000  \u001B[0m\u001B[101mERROR\u001B[0m\u001B[31m                 [\u001B[0m\u001B[101mtest.scope\u001B[0m] \u001B[0m\u001B[31mmessage\u001B[0m", //nolint:lll
		},
		{
			l: &simpleLogger{
				namespaceMaxLen: 26,
				coloring:        false,
				clock:           clockwork.NewFakeClock(),
			},
			msg: "test",
			exp: "1984-04-04 00:00:00.000  ERROR                 [test.scope] message",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.exp, tt.l.format([]string{"test", "scope"}, "message", ERROR))
		})
	}
}
