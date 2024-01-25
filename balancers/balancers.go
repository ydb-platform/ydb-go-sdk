package balancers

import (
	"sort"
	"strings"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

// Deprecated: RoundRobin is RandomChoice now
func RoundRobin() *balancerConfig.Config {
	return &balancerConfig.Config{}
}

func RandomChoice() *balancerConfig.Config {
	return &balancerConfig.Config{}
}

func SingleConn() *balancerConfig.Config {
	return &balancerConfig.Config{
		SingleConn: true,
	}
}

type filterLocalDC struct{}

func (filterLocalDC) Allow(info balancerConfig.Info, c conn.Conn) bool {
	return c.Endpoint().Location() == info.SelfLocation
}

func (filterLocalDC) String() string {
	return "LocalDC"
}

// PreferLocalDC creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// PreferLocalDC balancer try to autodetect local DC from client side.
func PreferLocalDC(balancer *balancerConfig.Config) *balancerConfig.Config {
	balancer.Filter = filterLocalDC{}
	balancer.DetectLocalDC = true
	return balancer
}

// PreferLocalDCWithFallBack creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocalDCWithFallBack(balancer *balancerConfig.Config) *balancerConfig.Config {
	balancer = PreferLocalDC(balancer)
	balancer.AllowFallback = true
	return balancer
}

type filterLocations []string

func (locations filterLocations) Allow(_ balancerConfig.Info, c conn.Conn) bool {
	location := strings.ToUpper(c.Endpoint().Location())
	for _, l := range locations {
		if location == l {
			return true
		}
	}
	return false
}

func (locations filterLocations) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("Locations{")
	for i, l := range locations {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(l)
	}
	buffer.WriteByte('}')

	return buffer.String()
}

// PreferLocations creates balancer which use endpoints only in selected locations (such as "ABC", "DEF", etc.)
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
func PreferLocations(balancer *balancerConfig.Config, locations ...string) *balancerConfig.Config {
	if len(locations) == 0 {
		panic("empty list of locations")
	}
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)
	balancer.Filter = filterLocations(locations)
	return balancer
}

// PreferLocationsWithFallback creates balancer which use endpoints only in selected locations
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocationsWithFallback(balancer *balancerConfig.Config, locations ...string) *balancerConfig.Config {
	balancer = PreferLocations(balancer, locations...)
	balancer.AllowFallback = true
	return balancer
}

type Endpoint interface {
	NodeID() uint32
	Address() string
	Location() string

	// Deprecated: LocalDC check "local" by compare endpoint location with discovery "selflocation" field.
	// It work good only if connection url always point to local dc.
	LocalDC() bool
}

type filterFunc func(info balancerConfig.Info, c conn.Conn) bool

func (p filterFunc) Allow(info balancerConfig.Info, c conn.Conn) bool {
	return p(info, c)
}

func (p filterFunc) String() string {
	return "Custom"
}

// Prefer creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
func Prefer(balancer *balancerConfig.Config, filter func(endpoint Endpoint) bool) *balancerConfig.Config {
	balancer.Filter = filterFunc(func(_ balancerConfig.Info, c conn.Conn) bool {
		return filter(c.Endpoint())
	})
	return balancer
}

// PreferWithFallback creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferWithFallback(balancer *balancerConfig.Config, filter func(endpoint Endpoint) bool) *balancerConfig.Config {
	balancer = Prefer(balancer, filter)
	balancer.AllowFallback = true
	return balancer
}

// Default balancer used by default
func Default() *balancerConfig.Config {
	return RandomChoice()
}
