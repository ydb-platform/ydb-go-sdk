package config

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"

// Dedicated package need for prevent cyclo dependencies config -> balancer -> config

type Config struct {
	IsPreferConn  PreferConnFunc
	AllowFalback  bool
	SingleConn    bool
	DetectlocalDC bool
	RoundRobin    bool
}

type Info struct {
	SelfLocation string
}

type PreferConnFunc func(info Info, c conn.Conn) bool
