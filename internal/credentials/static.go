package credentials

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Auth_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Auth"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/secret"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

var (
	_ Credentials             = (*Static)(nil)
	_ fmt.Stringer            = (*Static)(nil)
	_ StaticCredentialsOption = grpcDialOptionsOption(nil)
)

type grpcDialOptionsOption []grpc.DialOption

func (opts grpcDialOptionsOption) ApplyStaticCredentialsOption(c *Static) {
	c.opts = opts
}

type StaticCredentialsOption interface {
	ApplyStaticCredentialsOption(c *Static)
}

func WithGrpcDialOptions(opts ...grpc.DialOption) grpcDialOptionsOption {
	return opts
}

func NewStaticCredentials(user, password, endpoint string, opts ...StaticCredentialsOption) *Static {
	c := &Static{
		user:       user,
		password:   password,
		endpoint:   endpoint,
		sourceInfo: stack.Record(1),
	}
	for _, opt := range opts {
		opt.ApplyStaticCredentialsOption(c)
	}

	return c
}

var (
	_ Credentials  = (*Static)(nil)
	_ fmt.Stringer = (*Static)(nil)
)

// Static implements Credentials interface with static
// authorization parameters.
type Static struct {
	user       string
	password   string
	endpoint   string
	opts       []grpc.DialOption
	token      string
	requestAt  time.Time
	mu         sync.Mutex
	sourceInfo string
}

func (c *Static) Token(ctx context.Context) (token string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if shouldUseCachedToken(c.requestAt) {
		return c.token, nil
	}

	cc, err := createGRPCClient(ctx, c.endpoint, c.opts...)
	if err != nil {
		return "", formatDialError(err)
	}
	defer closeClient(cc)

	response, err := requestLogin(ctx, cc, c.user, c.password)
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	token, err = processLoginResponse(response, c.endpoint)
	if err != nil {
		return "", err
	}

	c.updateTokenCache(token)
	return c.token, nil
}

func shouldUseCachedToken(requestAt time.Time) bool {
	return time.Until(requestAt) > 0
}

func createGRPCClient(ctx context.Context, endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, endpoint, opts...)
}

func formatDialError(err error) error {
	return xerrors.WithStackTrace(fmt.Errorf("dial failed: %w", err))
}

func closeClient(cc *grpc.ClientConn) {
	_ = cc.Close()
}

func requestLogin(ctx context.Context, cc *grpc.ClientConn, user, password string) (*Ydb_Auth.LoginResponse, error) {
	client := Ydb_Auth_V1.NewAuthServiceClient(cc)
	return client.Login(ctx, &Ydb_Auth.LoginRequest{
		OperationParams: &Ydb_Operations.OperationParams{
			OperationMode:    0,
			OperationTimeout: nil,
			CancelAfter:      nil,
			Labels:           nil,
			ReportCostInfo:   0,
		},
		User:     user,
		Password: password,
	})
}

func processLoginResponse(response *Ydb_Auth.LoginResponse, endpoint string) (string, error) {
	switch {
	case !response.GetOperation().GetReady():
		return "", formatOperationNotReadyError(response)
	case response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
		return "", formatOperationStatusError(response, endpoint)
	}

	return extractTokenFromResponse(response)
}

func formatOperationNotReadyError(response *Ydb_Auth.LoginResponse) error {
	return xerrors.WithStackTrace(fmt.Errorf("operation '%s' not ready: %v",
		response.GetOperation().GetId(),
		response.GetOperation().GetIssues(),
	))
}

func formatOperationStatusError(response *Ydb_Auth.LoginResponse, endpoint string) error {
	return xerrors.WithStackTrace(
		xerrors.Operation(
			xerrors.FromOperation(response.GetOperation()),
			xerrors.WithAddress(endpoint),
		),
	)
}

func extractTokenFromResponse(response *Ydb_Auth.LoginResponse) (string, error) {
	var result Ydb_Auth.LoginResult
	if err := response.GetOperation().GetResult().UnmarshalTo(&result); err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	return result.GetToken(), nil
}

func (c *Static) updateTokenCache(token string) {
	expiresAt, err := parseExpiresAt(token)
	if err != nil {
		return
	}

	c.requestAt = time.Now().Add(time.Until(expiresAt) / 10)
	c.token = token
}

func parseExpiresAt(raw string) (expiresAt time.Time, err error) {
	var claims jwt.RegisteredClaims
	if _, _, err = jwt.NewParser().ParseUnverified(raw, &claims); err != nil {
		return expiresAt, xerrors.WithStackTrace(err)
	}

	return claims.ExpiresAt.Time, nil
}

func (c *Static) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteString("Static{User:")
	fmt.Fprintf(buffer, "%q", c.user)
	buffer.WriteString(",Password:")
	fmt.Fprintf(buffer, "%q", secret.Password(c.password))
	buffer.WriteString(",Token:")
	fmt.Fprintf(buffer, "%q", secret.Token(c.token))
	if c.sourceInfo != "" {
		buffer.WriteString(",From:")
		fmt.Fprintf(buffer, "%q", c.sourceInfo)
	}
	buffer.WriteByte('}')

	return buffer.String()
}
