package secret

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMask(t *testing.T) {
	for _, tt := range []struct {
		s   string
		exp string
	}{
		{
			s:   "test",
			exp: "****",
		},
		{
			s:   "test-long-password",
			exp: "t****************d",
		},
		{
			s:   "пароль",
			exp: "п****ь",
		},
		{
			s:   "пар",
			exp: "***",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.exp, Mask(tt.s))
		})
	}
}
