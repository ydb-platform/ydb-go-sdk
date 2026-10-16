// Package grpcclient connects server experiments directly to a gRPC endpoint.
package grpcclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const secureScheme = "grpcs"

type Conn struct {
	*grpc.ClientConn

	Database string
}

// Open performs no discovery, session pooling, or application-level retries.
// Authentication and TLS settings use the research runner's environment variables.
func Open(dsn string, options ...grpc.DialOption) (*Conn, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse YDB connection string: %w", err)
	}
	if (u.Scheme != "grpc" && u.Scheme != secureScheme) || u.Host == "" || u.Path == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("connection string must be grpc[s]://host:port/database")
	}
	transport, err := transportCredentials(u.Scheme)
	if err != nil {
		return nil, err
	}

	base := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithDisableRetry(),
		grpc.WithPerRPCCredentials(requestMetadata{
			database: u.Path, token: os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		}),
	}
	conn, err := grpc.NewClient("passthrough:///"+u.Host, append(base, options...)...)
	if err != nil {
		return nil, fmt.Errorf("open gRPC connection: %w", err)
	}

	return &Conn{ClientConn: conn, Database: u.Path}, nil
}

func transportCredentials(scheme string) (credentials.TransportCredentials, error) {
	if scheme != secureScheme {
		return insecure.NewCredentials(), nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	certificateFile := os.Getenv("YDB_SSL_ROOT_CERTIFICATES_FILE")
	if certificateFile == "" {
		return credentials.NewTLS(config), nil
	}
	pem, err := os.ReadFile(certificateFile)
	if err != nil {
		return nil, fmt.Errorf("read root certificates: %w", err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("load system root certificates: %w", err)
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("root certificate file contains no certificates")
	}
	config.RootCAs = roots

	return credentials.NewTLS(config), nil
}

type requestMetadata struct {
	database string
	token    string
}

func (m requestMetadata) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	md := map[string]string{"x-ydb-database": m.database}
	if m.token != "" {
		md["x-ydb-auth-ticket"] = m.token
	}

	return md, nil
}

func (requestMetadata) RequireTransportSecurity() bool { return false }

// StatusError retains the complete server issue tree without SDK classification.
type StatusError struct {
	Method string
	Status Ydb.StatusIds_StatusCode
	Issues []*Ydb_Issue.IssueMessage
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s returned %s: %v", e.Method, e.Status, e.Issues)
}

func CheckStatus(method string, status Ydb.StatusIds_StatusCode, issues []*Ydb_Issue.IssueMessage) error {
	if status != Ydb.StatusIds_SUCCESS {
		return &StatusError{Method: method, Status: status, Issues: issues}
	}

	return nil
}

func DecodeOperation(method string, operation *Ydb_Operations.Operation, result proto.Message) error {
	if operation == nil || !operation.GetReady() {
		return fmt.Errorf("%s returned no completed synchronous operation: %v", method, operation)
	}
	if err := CheckStatus(method, operation.GetStatus(), operation.GetIssues()); err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	if operation.GetResult() == nil {
		return fmt.Errorf("%s returned no result", method)
	}
	if err := operation.GetResult().UnmarshalTo(result); err != nil {
		return fmt.Errorf("decode %s result: %w", method, err)
	}

	return nil
}
