package log

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/secret"
)

func TestLazyProtoStringiferUpdateTokenRequest(t *testing.T) {
	const token = "3:serv:CMmNAXXXXXXSA"

	message := &Ydb_Topic.StreamWriteMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamWriteMessage_FromClient_UpdateTokenRequest{
			UpdateTokenRequest: &Ydb_Topic.UpdateTokenRequest{
				Token: token,
			},
		},
	}

	formatted := lazyProtoStringifer{message: message}.String()

	require.NotContains(t, formatted, token)
	require.Contains(t, formatted, secret.Token(token))
	require.True(t, strings.Contains(formatted, "updateTokenRequest"))
	require.Equal(t, token, message.GetUpdateTokenRequest().GetToken())
}
