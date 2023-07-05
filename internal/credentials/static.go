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
)

type staticCredentialsConfig interface {
	Endpoint() string
	GrpcDialOptions() []grpc.DialOption
}

func NewStaticCredentials(user, password string, config staticCredentialsConfig, opts ...Option) *Static {
	options := optionsHolder{
		sourceInfo: stack.Record(1),
	}
	for _, opt := range opts {
		opt(&options)
	}
	return &Static{
		user:       user,
		password:   password,
		endpoint:   config.Endpoint(),
		sourceInfo: options.sourceInfo,
		opts:       config.GrpcDialOptions(),
	}
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
	if time.Until(c.requestAt) > 0 {
		return c.token, nil
	}
	cc, err := grpc.DialContext(ctx, c.endpoint, c.opts...)
	if err != nil {
		return "", xerrors.WithStackTrace(
			fmt.Errorf("dial failed: %w", err),
		)
	}
	defer func() {
		_ = cc.Close()
	}()

	client := Ydb_Auth_V1.NewAuthServiceClient(cc)

	response, err := client.Login(ctx, &Ydb_Auth.LoginRequest{
		OperationParams: &Ydb_Operations.OperationParams{
			OperationMode:    0,
			OperationTimeout: nil,
			CancelAfter:      nil,
			Labels:           nil,
			ReportCostInfo:   0,
		},
		User:     c.user,
		Password: c.password,
	})
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	switch {
	case !response.GetOperation().GetReady():
		return "", xerrors.WithStackTrace(
			fmt.Errorf("operation '%s' not ready: %v",
				response.GetOperation().GetId(),
				response.GetOperation().GetIssues(),
			),
		)

	case response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
		return "", xerrors.WithStackTrace(
			xerrors.Operation(
				xerrors.FromOperation(response.GetOperation()),
				xerrors.WithNodeAddress(c.endpoint),
			),
		)
	}
	var result Ydb_Auth.LoginResult
	if err = response.GetOperation().GetResult().UnmarshalTo(&result); err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	expiresAt, err := parseExpiresAt(result.GetToken())
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	c.requestAt = time.Now().Add(time.Until(expiresAt) / 2)
	c.token = result.GetToken()

	return c.token, nil
}

func parseExpiresAt(raw string) (expiresAt time.Time, err error) {
	var claims jwt.RegisteredClaims
	if _, _, err = jwt.NewParser().ParseUnverified(raw, &claims); err != nil {
		return expiresAt, xerrors.WithStackTrace(err)
	}
	return claims.ExpiresAt.Time, nil
}

func (c *Static) String() string {
	return fmt.Sprintf(
		"Static(user:%q,password:%q,token:%q,from:%q)",
		c.user,
		secret.Password(c.password),
		secret.Token(c.token),
		c.sourceInfo,
	)
}
