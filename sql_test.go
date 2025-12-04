package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeDSN(t *testing.T) {
	for _, tt := range []struct {
		name     string
		dsn      string
		expected string
	}{
		{
			name:     "no userinfo",
			dsn:      "grpc://localhost:2135/local",
			expected: "grpc://localhost:2135/local",
		},
		{
			name:     "username only",
			dsn:      "grpc://user@localhost:2135/local",
			expected: "grpc://user@localhost:2135/local",
		},
		{
			name:     "username and password",
			dsn:      "grpc://user:password@localhost:2135/local",
			expected: "grpc://user:%2A%2A%2A@localhost:2135/local",
		},
		{
			name:     "username and empty password",
			dsn:      "grpc://user:@localhost:2135/local",
			expected: "grpc://user:%2A%2A%2A@localhost:2135/local",
		},
		{
			name:     "secure connection with password",
			dsn:      "grpcs://admin:secret123@ydb.example.com:2135/mydb",
			expected: "grpcs://admin:%2A%2A%2A@ydb.example.com:2135/mydb",
		},
		{
			name:     "with query parameters",
			dsn:      "grpc://user:pass@localhost:2135/local?query_mode=scripting",
			expected: "grpc://user:%2A%2A%2A@localhost:2135/local?query_mode=scripting",
		},
		{
			name:     "invalid url returns original",
			dsn:      "not a valid url :",
			expected: "not a valid url :",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeDSN(tt.dsn)
			require.Equal(t, tt.expected, result)
		})
	}
}
