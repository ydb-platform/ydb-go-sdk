package iam

import (
	"context"
	"crypto/x509"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/yandex-cloud/ydb-go-sdk/auth/iam/internal/genproto/iam/v1"
)

type grpcTransport struct {
	endpoint string
	certPool *x509.CertPool
}

func (t *grpcTransport) CreateToken(ctx context.Context, jwt string) (
	token string, expires time.Time, err error,
) {
	conn, err := t.conn(ctx)
	if err != nil {
		return
	}
	defer conn.Close()

	client := v1.NewIamTokenServiceClient(conn)
	res, err := client.Create(ctx, &v1.CreateIamTokenRequest{
		Identity: &v1.CreateIamTokenRequest_Jwt{
			Jwt: jwt,
		},
	})
	if err == nil {
		token = res.IamToken
		expires = time.Unix(
			res.ExpiresAt.Seconds,
			int64(res.ExpiresAt.Nanos),
		)
	}
	return
}

func (t *grpcTransport) conn(ctx context.Context) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	if pool := t.certPool; pool != nil {
		opts = []grpc.DialOption{
			grpc.WithTransportCredentials(
				credentials.NewClientTLSFromCert(pool, ""),
			),
		}
	} else {
		opts = []grpc.DialOption{
			grpc.WithInsecure(),
		}
	}
	return grpc.DialContext(ctx, t.endpoint, opts...)
}
