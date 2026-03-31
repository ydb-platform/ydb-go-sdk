package meta

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
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
		var buildInfo buildInfo

		if frameworks := m.buildInfo.Load().frameworks; len(frameworks) > 0 {
			buildInfo.frameworks = make(map[string]string, len(frameworks)+1)
			for frameworkName, frameworkVersion := range frameworks {
				buildInfo.frameworks[frameworkName] = frameworkVersion
			}
			buildInfo.frameworks[frameworkName] = frameworkVersion
		} else {
			buildInfo.frameworks = map[string]string{
				frameworkName: frameworkVersion,
			}
		}

		buildInfo.buildInfoHeader = buildInfoFirstPart + ";" + strings.Join(
			xslices.Transform(
				xslices.Keys(buildInfo.frameworks),
				func(frameworkName string) string {
					return frameworkName + "/" + buildInfo.frameworks[frameworkName]
				},
			), ";",
		)

		m.buildInfo.Store(&buildInfo)
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
		md.Set(HeaderVersion, m.buildInfo.Load().buildInfoHeader)
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
	md, err := m.meta(ctx)
	if err != nil {
		return ctx, xerrors.WithStackTrace(err)
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}
