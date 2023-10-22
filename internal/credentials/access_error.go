package credentials

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type authErrorOption interface {
	applyAuthErrorOption(buffer *bytes.Buffer)
}

var (
	_ authErrorOption = nodeIDAuthErrorOption(0)
	_ authErrorOption = addressAuthErrorOption("")
	_ authErrorOption = endpointAuthErrorOption("")
	_ authErrorOption = databaseAuthErrorOption("")
)

type addressAuthErrorOption string

func (address addressAuthErrorOption) applyAuthErrorOption(buffer *bytes.Buffer) {
	buffer.WriteString("address:")
	fmt.Fprintf(buffer, "%q", address)
}

func WithAddress(address string) addressAuthErrorOption {
	return addressAuthErrorOption(address)
}

type endpointAuthErrorOption string

func (endpoint endpointAuthErrorOption) applyAuthErrorOption(buffer *bytes.Buffer) {
	buffer.WriteString("endpoint:")
	fmt.Fprintf(buffer, "%q", endpoint)
}

func WithEndpoint(endpoint string) endpointAuthErrorOption {
	return endpointAuthErrorOption(endpoint)
}

type databaseAuthErrorOption string

func (address databaseAuthErrorOption) applyAuthErrorOption(buffer *bytes.Buffer) {
	buffer.WriteString("database:")
	fmt.Fprintf(buffer, "%q", address)
}

func WithDatabase(database string) databaseAuthErrorOption {
	return databaseAuthErrorOption(database)
}

type nodeIDAuthErrorOption uint32

func (id nodeIDAuthErrorOption) applyAuthErrorOption(buffer *bytes.Buffer) {
	buffer.WriteString("nodeID:")
	buffer.WriteString(strconv.FormatUint(uint64(id), 10))
}

func WithNodeID(id uint32) authErrorOption {
	return nodeIDAuthErrorOption(id)
}

type credentialsUnauthenticatedErrorOption struct {
	credentials Credentials
}

func (opt credentialsUnauthenticatedErrorOption) applyAuthErrorOption(buffer *bytes.Buffer) {
	buffer.WriteString("credentials:")
	if stringer, has := opt.credentials.(fmt.Stringer); has {
		fmt.Fprintf(buffer, "%q", stringer.String())
	} else {
		t := reflect.TypeOf(opt.credentials)
		fmt.Fprintf(buffer, "%q", t.PkgPath()+"."+t.Name())
	}
}

func WithCredentials(credentials Credentials) credentialsUnauthenticatedErrorOption {
	return credentialsUnauthenticatedErrorOption{
		credentials: credentials,
	}
}

func AccessError(msg string, err error, opts ...authErrorOption) error {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	buffer.WriteString(msg)
	buffer.WriteString(" (")
	for i, opt := range opts {
		if i != 0 {
			buffer.WriteString(",")
		}
		opt.applyAuthErrorOption(buffer)
	}
	buffer.WriteString("): %w")
	return xerrors.WithStackTrace(fmt.Errorf(buffer.String(), err), xerrors.WithSkipDepth(1))
}

func IsAccessError(err error) bool {
	if xerrors.IsTransportError(err,
		grpcCodes.Unauthenticated,
		grpcCodes.PermissionDenied,
	) {
		return true
	}
	if xerrors.IsOperationError(err,
		Ydb.StatusIds_UNAUTHORIZED,
	) {
		return true
	}
	return false
}
