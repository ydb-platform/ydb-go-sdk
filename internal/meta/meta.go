package meta

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/secret"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
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
		buildInfo:   xsync.NewValue(version.FullVersion),
	}
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

// WithBuildInfo adds framework name with its version to x-ydb-sdk-build-info header for all API requests.
func WithBuildInfo(frameworkName string, version string) Option {
	return func(m *Meta) {
		m.AppendBuildInfo(frameworkName, version)
	}
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

type Meta struct {
	pid             string
	trace           *trace.Driver
	credentials     credentials.Credentials
	database        string
	buildInfo       *xsync.Value[string]
	requestsType    string
	applicationName string
	capabilities    []string
}

func (m *Meta) meta(ctx context.Context) (_ metadata.MD, err error) {
	md, has := metadata.FromOutgoingContext(ctx)
	if !has {
		md = metadata.MD{}
	}

	md.Set(HeaderClientPid, m.pid)

	if len(md.Get(HeaderDatabase)) == 0 {
		md.Set(HeaderDatabase, m.database)
	}

	if len(md.Get(HeaderVersion)) == 0 {
		md.Set(HeaderVersion, m.buildInfo.Get())
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
	done := trace.DriverOnGetCredentials(m.trace, &ctx,
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

// AppendBuildInfo adds build information for the given framework.
// Note: duplicates are ignored – if called multiple times with the same
// framework name, only the value from the first call is kept. This is due
// to xsync.Map.Set being implemented via LoadOrStore and not overwriting
// existing values.
func (m *Meta) AppendBuildInfo(framework string, version string) {
	m.buildInfo.Change(func(old string) (new string) {
		parts := strings.Split(old, ";")
		frameworks := make(map[string]string, len(parts)+1)
		for _, part := range parts[1:] {
			subparts := strings.Split(part, "/")
			framework, version := strings.Join(subparts[:len(subparts)-1], "/"), subparts[len(subparts)-1]
			frameworks[framework] = version
		}

		frameworks[framework] = version

		return parts[0] + ";" + strings.Join(xslices.Transform(
			xslices.Uniq(xslices.Keys(frameworks)),
			func(framework string) string {
				return framework + "/" + frameworks[framework]
			},
		), ";")
	})
}

func (m *Meta) Context(ctx context.Context) (_ context.Context, err error) {
	md, err := m.meta(ctx)
	if err != nil {
		return ctx, xerrors.WithStackTrace(err)
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}
