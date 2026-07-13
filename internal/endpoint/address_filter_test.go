package endpoint

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddressFilter(t *testing.T) {
	filter := func(address string) bool {
		return address == "[2001:db8::1]:2135"
	}
	endpoint := New("node.example:2135", WithAddressFilter("IPv6", filter))

	require.Equal(t, "IPv6", endpoint.Key().AddressFilterKey)
	require.True(t, AddressFilter(endpoint)("[2001:db8::1]:2135"))
	require.False(t, AddressFilter(endpoint)("192.0.2.1:2135"))

	copy := endpoint.Copy()
	require.Equal(t, endpoint.Key(), copy.Key())
	require.True(t, AddressFilter(copy)("[2001:db8::1]:2135"))

	require.Nil(t, AddressFilter(nil))
}
