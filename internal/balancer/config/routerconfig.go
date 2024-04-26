package config

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

// Dedicated package need for prevent cyclo dependencies config -> balancer -> config

type Config struct {
	Filter        Filter
	AllowFallback bool
	SingleConn    bool
	DetectLocalDC bool
}

func (c Config) String() string {
	if c.SingleConn {
		return "SingleConn"
	}

	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("RandomChoice{")

	buffer.WriteString("DetectLocalDC=")
	fmt.Fprintf(buffer, "%t", c.DetectLocalDC)

	buffer.WriteString(",AllowFallback=")
	fmt.Fprintf(buffer, "%t", c.AllowFallback)

	if c.Filter != nil {
		buffer.WriteString(",Filter=")
		fmt.Fprint(buffer, c.Filter.String())
	}

	buffer.WriteByte('}')

	return buffer.String()
}

type Info struct {
	SelfLocation string
}

type Filter interface {
	Allow(info Info, c conn.Conn) bool
	String() string
}
