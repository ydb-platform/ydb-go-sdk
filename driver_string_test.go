package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestDriver_String(t *testing.T) {
	for _, tt := range []struct {
		name string
		d    *Driver
		s    string
	}{
		{
			name: xtest.CurrentFileLine(),
			d: &Driver{config: config.New(
				config.WithEndpoint("localhost"),
				config.WithDatabase("local"),
				config.WithSecure(false),
			)},
			s: `Driver{Endpoint:"localhost",Database:"local",Secure:false,Credentials:Anonymous{From:"github.com/ydb-platform/ydb-go-sdk/v3/config.defaultConfig(defaults.go:108)"}}`, //nolint:lll
		},
		{
			name: xtest.CurrentFileLine(),
			d: &Driver{config: config.New(
				config.WithEndpoint("localhost"),
				config.WithDatabase("local"),
				config.WithSecure(true),
			)},
			s: `Driver{Endpoint:"localhost",Database:"local",Secure:true,Credentials:Anonymous{From:"github.com/ydb-platform/ydb-go-sdk/v3/config.defaultConfig(defaults.go:108)"}}`, //nolint:lll
		},
		{
			name: xtest.CurrentFileLine(),
			d: &Driver{config: config.New(
				config.WithEndpoint("localhost"),
				config.WithDatabase("local"),
				config.WithSecure(false),
				config.WithCredentials(credentials.NewAnonymousCredentials(credentials.WithSourceInfo(t.Name()))),
			)},
			s: `Driver{Endpoint:"localhost",Database:"local",Secure:false,Credentials:Anonymous{From:"TestDriver_String"}}`, //nolint:lll
		},
		{
			name: xtest.CurrentFileLine(),
			d: &Driver{config: config.New(
				config.WithEndpoint("localhost"),
				config.WithDatabase("local"),
				config.WithSecure(true),
				config.WithCredentials(credentials.NewStaticCredentials("user", "password", "")),
			)},
			s: `Driver{Endpoint:"localhost",Database:"local",Secure:true,Credentials:Static{User:"user",Password:"pas***rd",Token:"****(CRC-32c: 00000000)",From:"github.com/ydb-platform/ydb-go-sdk/v3/credentials.NewStaticCredentials(credentials.go:35)"}}`, //nolint:lll
		},
		{
			name: xtest.CurrentFileLine(),
			d: &Driver{config: config.New(
				config.WithEndpoint("localhost"),
				config.WithDatabase("local"),
				config.WithSecure(true),
				config.WithCredentials(credentials.NewAccessTokenCredentials("AUTH_TOKEN")),
			)},
			s: `Driver{Endpoint:"localhost",Database:"local",Secure:true,Credentials:AccessToken{Token:"****(CRC-32c: 9F26E847)",From:"github.com/ydb-platform/ydb-go-sdk/v3/credentials.NewAccessTokenCredentials(credentials.go:20)"}}`, //nolint:lll
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.s, tt.d.String())
		})
	}
}
