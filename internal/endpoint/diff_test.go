package endpoint

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func endpointsToAddresses(endpoints []Endpoint) (addresses []string) {
	for _, endpoint := range endpoints {
		addresses = append(addresses, endpoint.String())
	}

	return addresses
}

func TestDiff(t *testing.T) {
	for _, tt := range []struct {
		name     string
		previous []Endpoint
		newest   []Endpoint
		steady   []Endpoint
		added    []Endpoint
		dropped  []Endpoint
	}{
		{
			name: "WithoutChanges",
			previous: []Endpoint{
				&endpoint{address: "2"},
				&endpoint{address: "1"},
				&endpoint{address: "0"},
				&endpoint{address: "3"},
			},
			newest: []Endpoint{
				&endpoint{address: "1"},
				&endpoint{address: "3"},
				&endpoint{address: "2"},
				&endpoint{address: "0"},
			},
			steady: []Endpoint{
				&endpoint{address: "0"},
				&endpoint{address: "1"},
				&endpoint{address: "2"},
				&endpoint{address: "3"},
			},
			added:   []Endpoint{},
			dropped: []Endpoint{},
		},
		{
			name: "OnlyAdded",
			previous: []Endpoint{
				&endpoint{address: "1"},
				&endpoint{address: "0"},
				&endpoint{address: "3"},
			},
			newest: []Endpoint{
				&endpoint{address: "1"},
				&endpoint{address: "3"},
				&endpoint{address: "2"},
				&endpoint{address: "0"},
			},
			steady: []Endpoint{
				&endpoint{address: "0"},
				&endpoint{address: "1"},
				&endpoint{address: "3"},
			},
			added: []Endpoint{
				&endpoint{address: "2"},
			},
			dropped: []Endpoint{},
		},
		{
			name: "OnlyDropped",
			previous: []Endpoint{
				&endpoint{address: "1"},
				&endpoint{address: "2"},
				&endpoint{address: "0"},
				&endpoint{address: "3"},
			},
			newest: []Endpoint{
				&endpoint{address: "1"},
				&endpoint{address: "3"},
				&endpoint{address: "0"},
			},
			steady: []Endpoint{
				&endpoint{address: "0"},
				&endpoint{address: "1"},
				&endpoint{address: "3"},
			},
			added: []Endpoint{},
			dropped: []Endpoint{
				&endpoint{address: "2"},
			},
		},
		{
			name: "AddedAndDropped",
			previous: []Endpoint{
				&endpoint{address: "4"},
				&endpoint{address: "7"},
				&endpoint{address: "8"},
			},
			newest: []Endpoint{
				&endpoint{address: "1"},
				&endpoint{address: "3"},
				&endpoint{address: "0"},
			},
			steady: []Endpoint{},
			added: []Endpoint{
				&endpoint{address: "0"},
				&endpoint{address: "1"},
				&endpoint{address: "3"},
			},
			dropped: []Endpoint{
				&endpoint{address: "4"},
				&endpoint{address: "7"},
				&endpoint{address: "8"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			steady, added, dropped := Diff(tt.previous, tt.newest)
			require.Equal(t, endpointsToAddresses(tt.added), endpointsToAddresses(added))
			require.Equal(t, endpointsToAddresses(tt.dropped), endpointsToAddresses(dropped))
			require.Equal(t, endpointsToAddresses(tt.steady), endpointsToAddresses(steady))
		})
	}
}
