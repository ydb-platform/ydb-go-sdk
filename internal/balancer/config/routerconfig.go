package config

import (
	"fmt"
	"net"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

// Dedicated package need for prevent cyclo dependencies config -> balancer -> config

// IPType is a bit-mask that controls which IP address families are permitted
// when the gRPC resolver resolves a FQDN target.
//
// Zero value (no bits set) means no filtering – both IPv4 and IPv6 addresses
// are used, which is the default SDK behaviour.
type IPType uint8

const (
	// IPv4 allows IPv4 addresses.
	IPv4 IPType = 1 << iota
	// IPv6 allows IPv6 addresses.
	IPv6

	// AllIPTypes is the default mask permitting both IPv4 and IPv6 addresses.
	AllIPTypes = IPv4 | IPv6
)

// Filter returns an address-filter function that accepts resolved IP:port
// strings and returns true if the address is permitted by the IPType mask.
//
// Returns nil when the mask is zero (no filtering) or when both IPv4 and IPv6
// are permitted (no filtering needed).
func (t IPType) Filter() func(addr string) bool {
	// No filter needed: zero mask or both families permitted.
	if t == 0 || t == AllIPTypes {
		return nil
	}
	return func(addr string) bool {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			// Not a host:port – pass through (should not happen from gRPC resolver).
			return true
		}

		ip := net.ParseIP(host)
		if ip == nil {
			// Cannot parse – pass through (FQDN or malformed; resolver should not
			// produce these, but let gRPC handle it).
			return true
		}

		isIPv4 := ip.To4() != nil

		if t&IPv4 != 0 && isIPv4 {
			return true
		}

		if t&IPv6 != 0 && !isIPv4 {
			return true
		}

		return false
	}
}

type Config struct {
	Filter          Filter
	AllowFallback   bool
	SingleConn      bool
	DetectNearestDC bool

	// AllowedIPTypes controls which IP address families the SDK uses when
	// connecting to cluster endpoints. Zero (default) means no restriction.
	AllowedIPTypes IPType
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
