/*
Package tvm provides interface for retrieving tvm tokens.
Uses tvmauth API client.
*/
package tvm

import (
	"context"

	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmauth"
)

const (
	YdbClientID = 2002490

	// https://lb.yandex-team.ru/docs/concepts/security/#tvm
	LogbrokerTvmClientID          = 2001059
	LogbrokerPrestableTvmClientID = 2001147
	LbkxTvmClientID               = 2001059
	LbkxtTvmClientID              = 2001147
	MessengerTvmClientID          = 2001059
)

type ClientConfig struct {
	// Who we are
	SelfID uint32
	// Where we connecting to
	ServiceID uint32
	// Connection secret
	Secret string
}

func Client(config ClientConfig, logger log.Logger) (ydb.Credentials, error) {
	ydbAlias := "ydb"

	settings := tvmauth.TvmAPISettings{
		SelfID: tvm.ClientID(config.SelfID),
		ServiceTicketOptions: tvmauth.NewAliasesOptions(
			config.Secret,
			map[string]tvm.ClientID{
				ydbAlias: tvm.ClientID(config.ServiceID),
			},
		),
	}

	c, err := tvmauth.NewAPIClient(settings, logger)
	if err != nil {
		return nil, err
	}
	return &client{
		ydbAlias: ydbAlias,
		client:   c,
	}, nil
}

type client struct {
	ydbAlias string
	client   *tvmauth.Client
}

func (c *client) Token(ctx context.Context) (token string, err error) {
	return c.client.GetServiceTicketForAlias(ctx, c.ydbAlias)
}
