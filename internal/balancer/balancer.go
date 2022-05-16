package balancer

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type (
	Balancer = *Config

	Config struct {
		IsPreferConn PreferConnFunc
		AllowFalback bool
		SingleConn   bool
	}
)

type PreferConnFunc func(c conn.Conn) bool

func IsOkConnection(c conn.Conn, bannedIsOk bool) bool {
	switch c.GetState() {
	case conn.Online, conn.Created, conn.Offline:
		return true
	case conn.Banned:
		return bannedIsOk
	default:
		return false
	}
}

type (
	NextOption         func(o *NextOptions)
	OnBadStateCallback func(ctx context.Context)

	NextOptions struct {
		OnBadState   OnBadStateCallback
		AcceptBanned bool
	}
)

func MakeNextOptions(opts ...NextOption) NextOptions {
	var o NextOptions
	for _, f := range opts {
		f(&o)
	}
	return o
}

func (o *NextOptions) Discovery(ctx context.Context) {
	if o.OnBadState != nil {
		o.OnBadState(ctx)
	}
}

func WithAcceptBanned(val bool) NextOption {
	return func(o *NextOptions) {
		o.AcceptBanned = val
	}
}

func WithOnBadState(callback OnBadStateCallback) NextOption {
	return func(o *NextOptions) {
		o.OnBadState = callback
	}
}
