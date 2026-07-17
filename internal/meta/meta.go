package meta

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/observability"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/secret"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var pid = os.Getpid()

func New(
	database string,
	credentials credentials.Credentials,
	trace *trace.Driver,
	opts ...Option,
) *Meta {
	m := &Meta{
		pid:         strconv.Itoa(pid),
		trace:       trace,
		credentials: credentials,
		database:    database,
	}

	m.buildInfo.Store(&buildInfo{
		frameworks:      make(map[string]string),
		buildInfoHeader: buildInfoFirstPart,
	})

	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}

	return m
}

type Option func(m *Meta)

func WithApplicationNameOption(applicationName string) Option {
	return func(m *Meta) {
		m.applicationName = applicationName
	}
}

func WithBuildInfo(frameworkName string, frameworkVersion string) Option {
	return func(m *Meta) {
		current := m.buildInfo.Load()
		next := &buildInfo{
			frameworks: make(map[string]string, len(current.frameworks)+1),
		}
		maps.Copy(next.frameworks, current.frameworks)
		next.frameworks[frameworkName] = frameworkVersion
		next.buildInfoHeader = next.makeHeader()
		m.buildInfo.Store(next)
	}
}

// makeHeader builds the session-facing x-ydb-sdk-build-info value:
// base SDK token plus custom frameworks. Observability adoption chains are
// intentionally omitted here so they do not pollute
// `.sys/query_sessions.ClientSdkBuildInfo`.
func (info *buildInfo) makeHeader() string {
	frameworkKeys := xslices.Keys(info.frameworks)
	frameworkKeys = xslices.Filter(frameworkKeys, func(frameworkName string) bool {
		return frameworkName != observability.TracingChainName &&
			frameworkName != observability.MetricsChainName
	})
	if len(frameworkKeys) == 0 {
		return buildInfoFirstPart
	}

	return buildInfoFirstPart + ";" + strings.Join(
		xslices.Transform(
			frameworkKeys,
			func(frameworkName string) string {
				return frameworkName + "/" + info.frameworks[frameworkName]
			},
		),
		";",
	)
}

// observabilityChains returns adoption markers for Discovery only, joined with ';'.
// Example: "ydb-sdk-tracing/0.1.0;ydb-sdk-metrics/0.1.0".
func (info *buildInfo) observabilityChains() string {
	var chains []string
	if version, ok := info.frameworks[observability.TracingChainName]; ok {
		chains = append(chains, observability.TracingChainName+"/"+version)
	}
	if version, ok := info.frameworks[observability.MetricsChainName]; ok {
		chains = append(chains, observability.MetricsChainName+"/"+version)
	}

	return strings.Join(chains, ";")
}

func WithRequestTypeOption(requestType string) Option {
	return func(m *Meta) {
		m.requestsType = requestType
	}
}

func AllowOption(feature string) Option {
	return func(m *Meta) {
		m.capabilities = append(m.capabilities, feature)
	}
}

func ForbidOption(feature string) Option {
	return func(m *Meta) {
		n := 0
		for _, capability := range m.capabilities {
			if capability != feature {
				m.capabilities[n] = capability
				n++
			}
		}
		m.capabilities = m.capabilities[:n]
	}
}

type (
	buildInfo struct {
		frameworks      map[string]string // frameworkName -> version
		buildInfoHeader string
	}
	Meta struct {
		pid             string
		trace           *trace.Driver
		credentials     credentials.Credentials
		database        string
		buildInfo       atomic.Pointer[buildInfo]
		requestsType    string
		applicationName string
		capabilities    []string
	}
)

const buildInfoFirstPart = version.FullVersion

func (m *Meta) meta(ctx context.Context, buildInfoHeader string) (_ metadata.MD, err error) {
	md, has := metadata.FromOutgoingContext(ctx)
	if !has {
		md = metadata.MD{}
	}

	md.Set(HeaderClientPid, m.pid)

	if len(md.Get(HeaderDatabase)) == 0 {
		md.Set(HeaderDatabase, m.database)
	}

	if len(md.Get(HeaderVersion)) == 0 {
		md.Set(HeaderVersion, buildInfoHeader)
	}

	if m.requestsType != "" {
		if len(md.Get(HeaderRequestType)) == 0 {
			md.Set(HeaderRequestType, m.requestsType)
		}
	}

	if m.applicationName != "" {
		md.Append(HeaderApplicationName, m.applicationName)
	}

	if len(m.capabilities) > 0 {
		md.Append(HeaderClientCapabilities, m.capabilities...)
	}

	if m.credentials == nil {
		return md, nil
	}

	var token string
	done := gtrace.DriverOnGetCredentials(m.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/meta.(*Meta).meta"),
	)
	defer func() {
		done(secret.Token(token), err)
	}()

	token, err = m.credentials.Token(ctx)
	if err != nil {
		if stringer, ok := m.credentials.(fmt.Stringer); ok {
			return nil, xerrors.WithStackTrace(fmt.Errorf("%w: %s", err, stringer.String()))
		}

		return nil, xerrors.WithStackTrace(err)
	}

	md.Set(HeaderTicket, token)

	return md, nil
}

func ValidateBuildInfo(frameworkName string, frameworkVersion string) error {
	if frameworkName == version.Package {
		return xerrors.WithStackTrace(errReservedPackageName)
	}

	if strings.ContainsAny(frameworkName, ";") {
		return xerrors.WithStackTrace(fmt.Errorf("%w: %q", errWrongFrameworkName, frameworkName))
	}

	if strings.ContainsAny(frameworkVersion, ";") {
		return xerrors.WithStackTrace(fmt.Errorf("%w: %q", errWrongFrameworkVersion, frameworkVersion))
	}

	return nil
}

func (m *Meta) Context(ctx context.Context) (_ context.Context, err error) {
	md, err := m.meta(ctx, m.buildInfo.Load().buildInfoHeader)
	if err != nil {
		return ctx, xerrors.WithStackTrace(err)
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}

// DiscoveryContext is like Context, but appends observability adoption chains to
// x-ydb-sdk-build-info. Use it only for Discovery.ListEndpoints.
func (m *Meta) DiscoveryContext(ctx context.Context) (_ context.Context, err error) {
	info := m.buildInfo.Load()
	header := info.buildInfoHeader
	if chains := info.observabilityChains(); chains != "" {
		header = buildInfoFirstPart + " " + chains + strings.TrimPrefix(info.buildInfoHeader, buildInfoFirstPart)
	}

	md, err := m.meta(ctx, header)
	if err != nil {
		return ctx, xerrors.WithStackTrace(err)
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}
