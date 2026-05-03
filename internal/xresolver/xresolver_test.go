package xresolver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// fakeCC records the last state passed to UpdateState.
type fakeCC struct {
	resolver.ClientConn

	lastState resolver.State
}

func (f *fakeCC) UpdateState(s resolver.State) error {
	f.lastState = s

	return nil
}

func (f *fakeCC) ReportError(_ error) {}

func TestClientConn_UpdateState_AddressFilter(t *testing.T) {
	tests := []struct {
		name      string
		input     []string
		filter    func(string) bool
		wantAddrs []string
	}{
		{
			name:      "NoFilter_AllAddressesPassThrough",
			input:     []string{"1.2.3.4:2135", "[::1]:2135"},
			filter:    nil,
			wantAddrs: []string{"1.2.3.4:2135", "[::1]:2135"},
		},
		{
			name:      "IPv6Filter_DropsIPv4",
			input:     []string{"1.2.3.4:2135", "[::1]:2135", "[2001:db8::1]:2135"},
			filter:    func(addr string) bool { return addr != "1.2.3.4:2135" },
			wantAddrs: []string{"[::1]:2135", "[2001:db8::1]:2135"},
		},
		{
			name:      "FilterDropsAll_EmptyResult",
			input:     []string{"1.2.3.4:2135", "5.6.7.8:2135"},
			filter:    func(addr string) bool { return false },
			wantAddrs: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeCC{}
			cc := &clientConn{
				ClientConn:    fake,
				trace:         &trace.Driver{},
				addressFilter: tc.filter,
			}

			addrs := make([]resolver.Address, len(tc.input))
			for i, a := range tc.input {
				addrs[i] = resolver.Address{Addr: a}
			}

			err := cc.UpdateState(resolver.State{Addresses: addrs})
			require.NoError(t, err)

			got := make([]string, len(fake.lastState.Addresses))
			for i, a := range fake.lastState.Addresses {
				got[i] = a.Addr
			}

			require.Equal(t, tc.wantAddrs, got)
		})
	}
}
