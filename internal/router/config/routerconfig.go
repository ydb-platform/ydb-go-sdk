package routerconfig

// Dedicated package need for prevent cyclo dependencies config -> router -> config

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type Config struct {
	IsPreferConn PreferConnFunc
	AllowFalback bool
	SingleConn   bool
}

type PreferConnFunc func(c conn.Conn) bool
