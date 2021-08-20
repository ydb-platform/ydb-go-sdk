package iam

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/yandex-cloud/go-genproto/yandex/cloud/iam/v1"
)

func TestGRPCCreateToken(t *testing.T) {
	const (
		token = "foo"
	)
	expires := time.Unix(0, 0)
	exp := timestamppb.New(expires)
	s := StubTokenService{
		OnCreate: func(ctx context.Context, req *v1.CreateIamTokenRequest) (
			res *v1.CreateIamTokenResponse, err error,
		) {
			return &v1.CreateIamTokenResponse{
				IamToken:  token,
				ExpiresAt: exp,
			}, nil
		},
	}
	addr, stop, err := s.ListenAndServe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stop(); err != nil {
			t.Fatalf("stop failed: %v", err)
		}
	}()

	gt := grpcTransport{
		endpoint: addr.String(),
		insecure: true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tk, e, err := gt.CreateToken(ctx, "jwt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if act, exp := e, expires; !act.Equal(exp) {
		t.Errorf("unexpected expiration time: %v; want %v", act, exp)
	}
	if act, exp := tk, token; act != exp {
		t.Errorf("unexpected token: %q; want %q", act, exp)
	}
}

type StubTokenService struct {
	OnCreate                  func(context.Context, *v1.CreateIamTokenRequest) (*v1.CreateIamTokenResponse, error)
	OnCreateForServiceAccount func(ctx context.Context, req *v1.CreateIamTokenForServiceAccountRequest) (*v1.CreateIamTokenResponse, error)
}

func (s *StubTokenService) ListenAndServe() (
	addr net.Addr,
	stop func() error,
	err error,
) {
	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		return nil, nil, err
	}

	srv := grpc.NewServer()
	v1.RegisterIamTokenServiceServer(srv, s)

	serve := make(chan error)
	go func() { serve <- srv.Serve(ln) }()

	return ln.Addr(), func() error {
		srv.GracefulStop()
		return <-serve
	}, nil
}

func (s StubTokenService) Create(ctx context.Context, req *v1.CreateIamTokenRequest) (res *v1.CreateIamTokenResponse, err error) {
	if f := s.OnCreate; f != nil {
		return f(ctx, req)
	}
	return nil, fmt.Errorf("stub: not implemented")
}

func (s StubTokenService) CreateForServiceAccount(ctx context.Context, req *v1.CreateIamTokenForServiceAccountRequest) (*v1.CreateIamTokenResponse, error) {
	if f := s.OnCreateForServiceAccount; f != nil {
		return f(ctx, req)
	}
	return nil, fmt.Errorf("stub: not implemented")
}
