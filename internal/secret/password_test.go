package secret

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPassword(t *testing.T) {
	for _, tt := range []struct {
		password string
		exp      string
	}{
		{
			password: "test",
			exp:      "****",
		},
		{
			password: "test-long-password",
			exp:      "tes*************rd",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.exp, Password(tt.password))
		})
	}
}
