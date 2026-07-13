package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIPVersionAddressFilter(t *testing.T) {
	require.Equal(t, "Unspecified", IPVersionUnspecified.String())
	require.Equal(t, "IPv6", IPv6.String())
	require.Equal(t, "IPVersion(42)", IPVersion(42).String())
	require.Contains(t, Config{IPVersion: IPv6}.String(), "IPVersion=IPv6")

	require.Nil(t, IPVersionUnspecified.AddressFilter())

	filter := IPv6.AddressFilter()
	require.NotNil(t, filter)
	for _, test := range []struct {
		name    string
		address string
		allowed bool
	}{
		{name: "IPv6", address: "[2001:db8::1]:2135", allowed: true},
		{name: "IPv4", address: "192.0.2.1:2135", allowed: false},
		{name: "IPv4MappedIPv6", address: "[::ffff:192.0.2.1]:2135", allowed: false},
		{name: "Hostname", address: "node.example:2135", allowed: false},
		{name: "MissingPort", address: "[2001:db8::1]", allowed: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.allowed, filter(test.address))
		})
	}

	unsupported := IPVersion(42).AddressFilter()
	require.NotNil(t, unsupported)
	require.False(t, unsupported("[2001:db8::1]:2135"))
}
