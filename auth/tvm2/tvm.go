/*
Package tvm provides interface for retrieving tvm tokens.
Uses tvm.Client client.
*/
package tvm2

import (
	"context"
	"sync"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/yandex/tvm"
)

// refreshInterval defines how frequntly TVM ticket should be updated.
var refreshInterval = time.Minute

// dstType is aimed only to distinguish requesting tickets with either alias or
// client ID.
type dstType int

const (
	dtAlias dstType = iota
	dtSrcID

	YdbClientID = 2002490

	// https://lb.yandex-team.ru/docs/concepts/security/#tvm
	LogbrokerTvmClientID          = 2001059
	LogbrokerPrestableTvmClientID = 2001147
	LbkxTvmClientID               = 2001059
	LbkxtTvmClientID              = 2001147
	MessengerTvmClientID          = 2001059
)

// TvmCredentials is a thin wrapper around client to TVM tool. Also, it
// runs background job to refresh the ticket.
type TvmCredentials struct {
	alias   string
	client  tvm.Client
	dstType dstType
	logger  log.Logger
	mutex   sync.Mutex
	srcID   tvm.ClientID
	ticket  string
}

var _ ydb.Credentials = (*TvmCredentials)(nil)

func NewTvmCredentialsForAlias(ctx context.Context, client tvm.Client, alias string, logger log.Logger) (*TvmCredentials, error) {
	return (&TvmCredentials{
		client:  client,
		dstType: dtAlias,
		logger:  logger,
		alias:   alias,
	}).init(ctx)
}

func NewTvmCredentialsForID(ctx context.Context, client tvm.Client, srcID uint32, logger log.Logger) (*TvmCredentials, error) {
	return (&TvmCredentials{
		client:  client,
		dstType: dtSrcID,
		logger:  logger,
		srcID:   tvm.ClientID(srcID),
	}).init(ctx)
}

func (p *TvmCredentials) Token(ctx context.Context) (string, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.ticket, nil
}

func (p *TvmCredentials) init(ctx context.Context) (*TvmCredentials, error) {
	if err := p.refreshToken(ctx); err != nil {
		return p, err
	}
	go p.runBackgroundJob(ctx)
	return p, nil
}

func (p *TvmCredentials) refreshToken(ctx context.Context) error {
	var err error
	var ticket string
	switch p.dstType {
	case dtAlias:
		ticket, err = p.client.GetServiceTicketForAlias(ctx, p.alias)
	case dtSrcID:
		ticket, err = p.client.GetServiceTicketForID(ctx, p.srcID)
	}

	if err == nil {
		p.mutex.Lock()
		p.ticket = ticket
		p.mutex.Unlock()
	}
	return err
}

func (p *TvmCredentials) runBackgroundJob(ctx context.Context) {
	var ticker = time.NewTicker(refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := p.refreshToken(ctx); err != nil {
				p.logger.Error("failed to refresh TVM tiket", log.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}
