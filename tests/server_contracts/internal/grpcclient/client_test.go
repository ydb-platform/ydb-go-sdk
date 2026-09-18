package grpcclient

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestConnectionMetadataOnUnaryAndStreamingRPCs(t *testing.T) {
	for _, scheme := range []string{"grpc", secureScheme} {
		t.Run(scheme, func(t *testing.T) {
			t.Setenv("YDB_ACCESS_TOKEN_CREDENTIALS", "test-token")
			checkMetadata := func(ctx context.Context) {
				md, _ := metadata.FromIncomingContext(ctx)
				for key, want := range map[string][]string{
					"x-ydb-database": {"/local"}, "x-ydb-auth-ticket": {"test-token"},
				} {
					if got := md.Get(key); !reflect.DeepEqual(got, want) {
						t.Errorf("metadata %q: got %v, want %v", key, got, want)
					}
				}
			}
			options := []grpc.ServerOption{
				grpc.UnaryInterceptor(func(
					ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler,
				) (any, error) {
					checkMetadata(ctx)

					return next(ctx, request)
				}),
				grpc.StreamInterceptor(func(
					srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler,
				) error {
					checkMetadata(stream.Context())

					return next(srv, stream)
				}),
			}
			if scheme == secureScheme {
				options = append(options, grpc.Creds(testServerCredentials(t)))
			}
			listener := bufconn.Listen(1024 * 1024)
			t.Cleanup(func() { _ = listener.Close() })
			server := grpc.NewServer(options...)
			grpc_health_v1.RegisterHealthServer(server, health.NewServer())
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			conn, err := Open(scheme+"://localhost:2136/local", grpc.WithContextDialer(
				func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) },
			))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			client := grpc_health_v1.NewHealthClient(conn)
			if _, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{}); err != nil {
				t.Fatal(err)
			}
			stream, err := client.Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Recv(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConnectionRejectsUnsupportedDSNs(t *testing.T) {
	for _, dsn := range []string{
		"http://localhost/local", "grpc:///local", "grpc://localhost", "grpc://localhost/local?unknown=1",
		"grpc://user:password@localhost/local", "grpc://localhost/local#fragment", "grpc://%",
	} {
		if conn, err := Open(dsn); err == nil {
			_ = conn.Close()
			t.Errorf("accepted unsupported DSN %q", dsn)
		}
	}
}

func testServerCredentials(t *testing.T) credentials.TransportCredentials {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), DNSNames: []string{"localhost"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	certificateFile := filepath.Join(t.TempDir(), "root.pem")
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certificateFile, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("YDB_SSL_ROOT_CERTIFICATES_FILE", certificateFile)

	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}},
	})
}
