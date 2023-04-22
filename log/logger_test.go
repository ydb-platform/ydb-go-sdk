package log

import (
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestColoring(t *testing.T) {
	for _, tt := range []struct {
		l   *logger
		msg string
		exp string
	}{
		{
			l: &logger{
				namespace:   []string{"ydb"},
				scopeMaxLen: 26,
				coloring:    true,
				clock:       clockwork.NewFakeClock(),
			},
			msg: "test",
			exp: "\u001B[31m1984-04-04 00:00:00.000  \u001B[0m\u001B[101mERROR\u001B[0m\u001B[31m             [\u001B[0m\u001B[101mydb.test.scope\u001B[0m] \u001B[0m\u001B[31mmessage\u001B[0m", //nolint:lll
		},
		{
			l: &logger{
				namespace:   []string{"ydb"},
				scopeMaxLen: 26,
				coloring:    false,
				clock:       clockwork.NewFakeClock(),
			},
			msg: "test",
			exp: "1984-04-04 00:00:00.000  ERROR             [ydb.test.scope] message",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.exp, tt.l.format([]string{"test", "scope"}, "message", ERROR))
		})
	}
}
