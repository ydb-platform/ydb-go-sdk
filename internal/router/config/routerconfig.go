package routerconfig

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"

// Dedicated package need for prevent cyclo dependencies config -> router -> config

type Config struct {
	IsPreferConn  PreferConnFunc
	AllowFalback  bool
	SingleConn    bool
	DetectlocalDC bool
}

type Info struct {
	SelfLocation string
}

type PreferConnFunc func(routerInfo Info, c conn.Conn) bool
