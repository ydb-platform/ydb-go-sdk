package config

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

// Dedicated package need for prevent cyclo dependencies config -> balancer -> config

type Config struct {
	Filter          Filter
	AllowFallback   bool
	SingleConn      bool
	DetectNearestDC bool

	// MaxConnections limits how many discovered endpoints the balancer keeps
	// dialed/active at once (sticky subset, similar to ydb-python-sdk).
	//
	// Zero means unlimited.
	//
	// Soft limit: balancers.WithNodeID / session affinity to a node outside the
	// active set, or CreateSession returning a different node than the one that
	// handled the RPC, may open additional connections beyond MaxConnections.
	MaxConnections int
}

func (c Config) String() string {
	if c.SingleConn {
		return "SingleConn"
	}

	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("RandomChoice{")

	buffer.WriteString("DetectNearestDC=")
	fmt.Fprintf(buffer, "%t", c.DetectNearestDC)

	buffer.WriteString(",AllowFallback=")
	fmt.Fprintf(buffer, "%t", c.AllowFallback)

	buffer.WriteString(",MaxConnections=")
	fmt.Fprintf(buffer, "%d", c.MaxConnections)

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
	Allow(info Info, e endpoint.Info) bool
	String() string
}
