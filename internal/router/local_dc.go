package router

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/gogroup"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

const (
	maxEndpointsCheckPerLocation = 5
)

func checkFastestAddress(ctx context.Context, addresses []string) (string, error) {
	results := make(chan string, len(addresses))

	grp := gogroup.WithContext(ctx, len(addresses))

	workCtx, workCancel := context.WithCancel(ctx)
	defer workCancel()

	dialer := net.Dialer{}
	createPingFunction := func(address string) func() error {
		return func() error {
			conn, err := dialer.DialContext(workCtx, "tcp", address)
			if err == nil {
				results <- address
				workCancel()
			}
			if conn != nil {
				_ = conn.Close()
			}
			return err
		}
	}
	for _, addr := range addresses {
		grp.Go(createPingFunction(addr))
	}

	err := grp.Wait()
	close(results)

	res := <-results
	if res == "" {
		return "", err
	}
	return res, nil
}

func detectFastestEndpoint(ctx context.Context, endpoints []endpoint.Endpoint) (endpoint.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, xerrors.WithStackTrace(errors.New("empty endpoints list"))
	}

	var lastErr error
	// common is 2 ip address for every fqdn: ipv4 + ipv6
	initialAddressToEndpointCapacity := len(endpoints) * 2
	addressToEndpoint := make(map[string]endpoint.Endpoint, initialAddressToEndpointCapacity)
	for _, ep := range endpoints {
		host, port, err := extractHostPort(ep.Address())
		if err != nil {
			lastErr = xerrors.WithStackTrace(err)
			continue
		}

		addresses, err := net.LookupHost(host)
		if err != nil {
			lastErr = err
			continue
		}
		if len(addresses) == 0 {
			lastErr = xerrors.WithStackTrace(fmt.Errorf("no ips for fqdn: %q", host))
			continue
		}

		for _, ip := range addresses {
			address := net.JoinHostPort(ip, port)
			addressToEndpoint[address] = ep
		}
	}
	if len(addressToEndpoint) == 0 {
		return nil, xerrors.WithStackTrace(lastErr)
	}
	addressesToPing := make([]string, 0, len(addressToEndpoint))
	for ip := range addressToEndpoint {
		addressesToPing = append(addressesToPing, ip)
	}

	fastestAddress, err := checkFastestAddress(ctx, addressesToPing)
	if err != nil {
		return nil, err
	}
	return addressToEndpoint[fastestAddress], nil
}

func detectLocalDC(ctx context.Context, endpoints []endpoint.Endpoint) (string, error) {
	if len(endpoints) == 0 {
		return "", xerrors.WithStackTrace(ErrClusterEmpty)
	}
	endpointsByDc := splitEndpointsByLocation(endpoints)

	if len(endpointsByDc) == 1 {
		return endpoints[0].Location(), nil
	}

	endpointsToTest := make([]endpoint.Endpoint, 0, maxEndpointsCheckPerLocation*len(endpointsByDc))
	for _, dcEndpoints := range endpointsByDc {
		endpointsToTest = append(endpointsToTest, getRandomEndpoints(dcEndpoints, maxEndpointsCheckPerLocation)...)
	}

	fastest, err := detectFastestEndpoint(ctx, endpointsToTest)
	if err == nil {
		return fastest.Location(), nil
	}
	return "", err
}

func extractHostPort(address string) (host string, port string, _ error) {
	if !strings.Contains(address, "://") {
		address = "stub://" + address
	}

	u, err := url.Parse(address)
	if err != nil {
		return "", "", xerrors.WithStackTrace(err)
	}
	host, port, err = net.SplitHostPort(u.Host)
	if err != nil {
		return "", "", xerrors.WithStackTrace(err)
	}
	return host, port, nil
}

func getRandomEndpoints(endpoints []endpoint.Endpoint, count int) []endpoint.Endpoint {
	if len(endpoints) <= count {
		return endpoints
	}

	got := make(map[int]bool, maxEndpointsCheckPerLocation)

	res := make([]endpoint.Endpoint, 0, maxEndpointsCheckPerLocation)
	for len(got) < count {
		//nolint:gosec
		index := rand.Intn(len(endpoints))
		if got[index] {
			continue
		}

		got[index] = true
		res = append(res, endpoints[index])
	}

	return res
}

func splitEndpointsByLocation(endpoints []endpoint.Endpoint) map[string][]endpoint.Endpoint {
	res := make(map[string][]endpoint.Endpoint)
	for _, ep := range endpoints {
		location := ep.Location()
		res[location] = append(res[location], ep)
	}
	return res
}
