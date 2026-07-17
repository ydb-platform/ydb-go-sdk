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
		frameworks:               make(map[string]string),
		buildInfoHeader:          buildInfoFirstPart,
		discoveryBuildInfoHeader: buildInfoFirstPart,
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

		next.buildInfoHeader = next.makeHeader(false)
		next.discoveryBuildInfoHeader = next.makeHeader(true)
		m.buildInfo.Store(next)
	}
}

// makeHeader keeps backward-compatible semantics for build-info parsers.
//
// Regular requests (includeObservability=false) keep session-facing headers lean:
// only the base SDK token and custom frameworks. Observability adoption chains are
// intentionally omitted so they do not pollute `.sys/query_sessions.ClientSdkBuildInfo`.
//
// Discovery requests (includeObservability=true) append chain markers:
// - chain markers are appended after the base token using a space;
// - tracing and metrics markers are separated with ';' when both are present;
// - custom frameworks remain in ';framework/version' form.
func (info *buildInfo) makeHeader(includeObservability bool) string {
	builder := strings.Builder{}
	builder.WriteString(buildInfoFirstPart)

	if includeObservability {
		info.writeObservabilityChains(&builder)
	}

	frameworkKeys := xslices.Keys(info.frameworks)
	frameworkKeys = xslices.Filter(frameworkKeys, func(frameworkName string) bool {
		return frameworkName != observability.TracingChainName &&
			frameworkName != observability.MetricsChainName
	})

	if len(frameworkKeys) == 0 {
		return builder.String()
	}

	builder.WriteString(";")
	builder.WriteString(
		strings.Join(
			xslices.Transform(
				frameworkKeys,
				func(frameworkName string) string {
					return frameworkName + "/" + info.frameworks[frameworkName]
				},
			), ";",
		),
	)

	return builder.String()
}

func (info *buildInfo) writeObservabilityChains(builder *strings.Builder) {
	tracingVersion, hasTracing := info.frameworks[observability.TracingChainName]
	if hasTracing {
		builder.WriteString(" ")
		builder.WriteString(observability.TracingChainName)
		builder.WriteString("/")
		builder.WriteString(tracingVersion)
	}

	metricsVersion, hasMetrics := info.frameworks[observability.MetricsChainName]
	if !hasMetrics {
		return
	}

	if hasTracing {
		builder.WriteString(";")
	} else {
		builder.WriteString(" ")
	}
	builder.WriteString(observability.MetricsChainName)
	builder.WriteString("/")
	builder.WriteString(metricsVersion)
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
		frameworks               map[string]string // frameworkName -> version
		buildInfoHeader          string
		discoveryBuildInfoHeader string
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

// DiscoveryContext is like Context, but includes observability adoption chains in
// x-ydb-sdk-build-info. Use it only for Discovery.ListEndpoints so session-facing
// RPCs (CreateSession and friends) keep a lean ClientSdkBuildInfo.
func (m *Meta) DiscoveryContext(ctx context.Context) (_ context.Context, err error) {
	md, err := m.meta(ctx, m.buildInfo.Load().discoveryBuildInfoHeader)
	if err != nil {
		return ctx, xerrors.WithStackTrace(err)
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}
