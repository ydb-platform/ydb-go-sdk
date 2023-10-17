package credentials

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type unauthenticatedErrorOption interface {
	applyUnauthenticatedErrorOption(buffer *bytes.Buffer)
}

var (
	_ unauthenticatedErrorOption = nodeIDUnauthenticatedErrorOption(0)
	_ unauthenticatedErrorOption = addressUnauthenticatedErrorOption("")
)

type addressUnauthenticatedErrorOption string

func (address addressUnauthenticatedErrorOption) applyUnauthenticatedErrorOption(buffer *bytes.Buffer) {
	buffer.WriteString("address:")
	fmt.Fprintf(buffer, "%q", address)
}

func WithAddress(address string) addressUnauthenticatedErrorOption {
	return addressUnauthenticatedErrorOption(address)
}

type endpointUnauthenticatedErrorOption string

func (endpoint endpointUnauthenticatedErrorOption) applyUnauthenticatedErrorOption(buffer *bytes.Buffer) {
	buffer.WriteString("endpoint:")
	fmt.Fprintf(buffer, "%q", endpoint)
}

func WithEndpoint(endpoint string) endpointUnauthenticatedErrorOption {
	return endpointUnauthenticatedErrorOption(endpoint)
}

type databaseUnauthenticatedErrorOption string

func (address databaseUnauthenticatedErrorOption) applyUnauthenticatedErrorOption(buffer *bytes.Buffer) {
	buffer.WriteString("database:")
	fmt.Fprintf(buffer, "%q", address)
}

func WithDatabase(database string) databaseUnauthenticatedErrorOption {
	return databaseUnauthenticatedErrorOption(database)
}

type nodeIDUnauthenticatedErrorOption uint32

func (id nodeIDUnauthenticatedErrorOption) applyUnauthenticatedErrorOption(buffer *bytes.Buffer) {
	buffer.WriteString("nodeID:")
	buffer.WriteString(strconv.FormatUint(uint64(id), 10))
}

func WithNodeID(id uint32) unauthenticatedErrorOption {
	return nodeIDUnauthenticatedErrorOption(id)
}

type credentialsUnauthenticatedErrorOption struct {
	credentials Credentials
}

func (opt credentialsUnauthenticatedErrorOption) applyUnauthenticatedErrorOption(buffer *bytes.Buffer) {
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

func UnauthenticatedError(msg string, err error, opts ...unauthenticatedErrorOption) error {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	buffer.WriteString(msg)
	buffer.WriteString(" (")
	for i, opt := range opts {
		if i != 0 {
			buffer.WriteString(",")
		}
		opt.applyUnauthenticatedErrorOption(buffer)
	}
	buffer.WriteString("): %w")
	return xerrors.WithStackTrace(fmt.Errorf(buffer.String(), err), xerrors.WithSkipDepth(1))
}
