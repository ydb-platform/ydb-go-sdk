package secret

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToken(t *testing.T) {
	for _, tt := range []struct {
		token string
		exp   string
	}{
		{
			token: "t1.9euelZqOnJiKlcuPzciQyJjNyZ6OzO3rnpWaj53JlZKcy8aTyoyYm8-Uz8ABguCyAUPnxXz391GzzByWXoOxBT7krgydRAkaqyL7G4VVZCQDN1lZ8Lxk_sKbBMpbRn51r8dxNJNzr80ai5AQ", //nolint:lll
			exp:   "t1.9****i5AQ(CRC-32c: B83EA6E4)",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.exp, Token(tt.token))
		})
	}
}
