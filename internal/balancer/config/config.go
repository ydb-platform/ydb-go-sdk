package config

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Dedicated package need for prevent cyclo dependencies config -> balancer -> config

type Config struct {
	filter        Filter
	allowFallback bool
	singleConn    bool
	detectLocalDC bool

	endpoint    string
	database    string
	credentials credentials.Credentials
	trace       trace.Driver
	dialTimeout time.Duration
	meta        meta.Meta
}

type filterFunc func(info Info, c conn.Info) bool

func (p filterFunc) Allow(info Info, c conn.Info) bool {
	return p(info, c)
}

func (p filterFunc) String() string {
	return "Custom"
}

type filterLocalDC struct{}

func (filterLocalDC) Allow(info Info, c conn.Info) bool {
	return c.Endpoint().Location() == info.SelfLocation
}

func (filterLocalDC) String() string {
	return "LocalDC"
}

type filterLocations []string

func (locations filterLocations) Allow(_ Info, c conn.Info) bool {
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

var defaultConfig = &Config{
	filter:        filterFunc(func(info Info, c conn.Info) bool { return true }),
	allowFallback: false,
	singleConn:    false,
	detectLocalDC: false,
	endpoint:      "",
	database:      "",
	credentials:   nil,
	trace:         trace.Driver{},
	dialTimeout:   0,
	meta:          meta.Meta{},
}

func New(opts ...Option) *Config {
	config := *defaultConfig
	for _, opt := range opts {
		opt(&config)
	}

	return &config
}

// Endpoint is a required starting endpoint for connect
func (c *Config) Endpoint() string {
	if c == nil {
		return defaultConfig.endpoint
	}

	return c.endpoint
}

// Database is a required database name.
func (c *Config) Database() string {
	if c == nil {
		return defaultConfig.database
	}

	return c.database
}

func (c *Config) Credentials() credentials.Credentials {
	if c == nil {
		return defaultConfig.credentials
	}

	return c.credentials
}

func (c *Config) Filter() Filter {
	if c == nil {
		return defaultConfig.filter
	}

	return c.filter
}

func (c *Config) SingleConn() bool {
	if c == nil {
		return defaultConfig.singleConn
	}

	return c.singleConn
}

func (c *Config) DetectLocalDC() bool {
	if c == nil {
		return defaultConfig.detectLocalDC
	}

	return c.detectLocalDC
}

func (c *Config) AllowFallback() bool {
	if c == nil {
		return defaultConfig.allowFallback
	}

	return c.allowFallback
}

// Trace contains driver tracing options.
func (c *Config) Trace() *trace.Driver {
	if c == nil {
		return &defaultConfig.trace
	}

	return &c.trace
}

// Meta reports meta information about database connection
func (c *Config) Meta() *meta.Meta {
	if c == nil {
		return &defaultConfig.meta
	}

	return &c.meta
}

// DialTimeout is the maximum amount of time a dial will wait for a connect to
// complete.
//
// If DialTimeout is zero then no timeout is used.
func (c *Config) DialTimeout() time.Duration {
	if c == nil {
		return defaultConfig.dialTimeout
	}

	return c.dialTimeout
}

func (c *Config) String() string {
	if c == nil {
		return defaultConfig.String()
	}

	if c.SingleConn() {
		return "SingleConn"
	}

	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("RandomChoice{")

	buffer.WriteString("DetectLocalDC=")
	fmt.Fprintf(buffer, "%t", c.DetectLocalDC())

	buffer.WriteString(",AllowFallback=")
	fmt.Fprintf(buffer, "%t", c.AllowFallback())

	buffer.WriteString(",Filter=")
	fmt.Fprint(buffer, c.Filter().String())

	buffer.WriteByte('}')

	return buffer.String()
}

type Option func(c *Config)

func WithEndpoint(endpoint string) Option {
	return func(c *Config) {
		c.endpoint = endpoint
	}
}

func FilterFunc(f func(info Info, c conn.Info) bool) Option {
	return func(c *Config) {
		c.filter = filterFunc(f)
	}
}

func FilterLocalDC() Option {
	return func(c *Config) {
		c.filter = filterLocalDC{}
	}
}

func FilterLocations(locations ...string) Option {
	if len(locations) == 0 {
		panic("empty list of locations")
	}
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)

	return func(c *Config) {
		c.filter = filterLocations(locations)
	}
}

func AllowFallback() Option {
	return func(c *Config) {
		c.allowFallback = true
	}
}

func DetectLocalDC() Option {
	return func(c *Config) {
		c.detectLocalDC = true
	}
}

func UseSingleConn() Option {
	return func(c *Config) {
		c.singleConn = true
	}
}

func WithDatabase(database string) Option {
	return func(c *Config) {
		c.database = database
	}
}

func WithCredentials(credentials credentials.Credentials) Option {
	return func(c *Config) {
		c.credentials = credentials
	}
}

func WithTrace(trace *trace.Driver) Option {
	return func(c *Config) {
		c.trace = *trace
	}
}

func WithDialTimeout(dialTimeout time.Duration) Option {
	return func(c *Config) {
		c.dialTimeout = dialTimeout
	}
}

func WithMeta(meta *meta.Meta) Option {
	return func(c *Config) {
		c.meta = *meta
	}
}

func (c *Config) With(opts ...Option) *Config {
	if c == nil {
		c = defaultConfig
	}
	config := *c
	for _, opt := range opts {
		opt(&config)
	}

	return &config
}

type (
	Info struct {
		SelfLocation string
	}
	Filter interface {
		Allow(info Info, c conn.Info) bool
		String() string
	}
)
