package config

import (
	"fmt"
	"net"
	"net/netip"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

// Dedicated package need for prevent cyclo dependencies config -> balancer -> config

type Config struct {
	Filter          Filter
	AllowFallback   bool
	SingleConn      bool
	DetectNearestDC bool
	IPVersion       IPVersion
}

type IPVersion uint8

const (
	IPVersionUnspecified IPVersion = iota
	IPv6
)

func (v IPVersion) String() string {
	switch v {
	case IPVersionUnspecified:
		return "Unspecified"
	case IPv6:
		return "IPv6"
	default:
		return fmt.Sprintf("IPVersion(%d)", v)
	}
}

// AddressFilter returns a resolver address filter for the requested IP version.
// A nil result means that no IP version restriction is configured.
func (v IPVersion) AddressFilter() func(string) bool {
	switch v {
	case IPVersionUnspecified:
		return nil
	case IPv6:
		return func(address string) bool {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return false
			}

			ip, err := netip.ParseAddr(host)

			return err == nil && ip.Is6() && !ip.Is4In6()
		}
	default:
		return func(string) bool {
			return false
		}
	}
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

	if c.IPVersion != IPVersionUnspecified {
		buffer.WriteString(",IPVersion=")
		fmt.Fprint(buffer, c.IPVersion)
	}

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
