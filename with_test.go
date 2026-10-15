package ydb //nolint:testpackage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/certificates"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestWithConnectionTTL(t *testing.T) {
	const ttl = 30 * time.Second

	db, err := driverFromOptions(t.Context(), WithConnectionTTL(ttl))
	require.NoError(t, err)
	require.Equal(t, ttl, db.config.ConnectionTTL())
}

func TestWithCertificatesCached(t *testing.T) {
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	require.NoError(t, err)
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)
	caPEM := new(bytes.Buffer)
	err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	require.NoError(t, err)
	f, err := os.CreateTemp(os.TempDir(), "ca.pem")
	defer os.Remove(f.Name())
	defer f.Close()
	require.NoError(t, err)
	_, err = f.Write(caPEM.Bytes())
	require.NoError(t, err)

	var (
		n           = 100
		hitCounter  uint64
		missCounter uint64
		ctx         = context.TODO()
	)
	for _, test := range []struct {
		name    string
		options []Option
		expMiss uint64
		expHit  uint64
	}{
		{
			"no cache",
			[]Option{},
			0,
			0,
		},
		{
			"file cache",
			[]Option{
				WithCertificatesFromFile(f.Name(),
					certificates.FromFileOnHit(func() {
						atomic.AddUint64(&hitCounter, 1)
					}),
					certificates.FromFileOnMiss(func() {
						atomic.AddUint64(&missCounter, 1)
					}),
				),
			},
			0,
			uint64(n),
		},
		{
			"pem cache",
			[]Option{
				WithCertificatesFromPem(caPEM.Bytes(),
					certificates.FromPemOnHit(func() {
						atomic.AddUint64(&hitCounter, 1)
					}),
					certificates.FromPemMiss(func() {
						atomic.AddUint64(&missCounter, 1)
					}),
				),
			},
			0,
			uint64(n),
		},
		{
			"pem&file cache",
			[]Option{
				WithCertificatesFromFile(f.Name(),
					certificates.FromFileOnHit(func() {
						atomic.AddUint64(&hitCounter, 1)
					}),
					certificates.FromFileOnMiss(func() {
						atomic.AddUint64(&missCounter, 1)
					}),
				),
				WithCertificatesFromPem(caPEM.Bytes(),
					certificates.FromPemOnHit(func() {
						atomic.AddUint64(&hitCounter, 1)
					}),
					certificates.FromPemMiss(func() {
						atomic.AddUint64(&missCounter, 1)
					}),
				),
			},
			0,
			uint64(n * 2),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := driverFromOptions(ctx,
				append(
					test.options,
					withConnPool(conn.NewPool(context.Background(), config.New())), //nolint:contextcheck
				)...,
			)
			require.NoError(t, err)

			hitCounter, missCounter = 0, 0

			for range n {
				_, _, err := db.with(ctx,
					func(ctx context.Context, c *Driver) error {
						return nil // nothing to do
					},
				)
				require.NoError(t, err)
			}
			require.Equal(t, test.expHit, hitCounter)
			require.Equal(t, test.expMiss, missCounter)
		})
	}
}

func TestFailedWithReleasesItsPoolReference(t *testing.T) {
	for _, depth := range []int{0, 1, 2} {
		for _, failOption := range []bool{false, true} {
			name := fmt.Sprintf("depth_%d/%s", depth, map[bool]string{false: "connect", true: "option"}[failOption])
			t.Run(name, func(t *testing.T) {
				var releases atomic.Int64

				driver, err := Open(context.Background(), "grpc://localhost:2135/missing",
					WithAnonymousCredentials(),
					WithBalancer(balancers.SingleConn()),
					WithTraceDriver(trace.Driver{
						OnPoolRelease: func(trace.DriverConnPoolReleaseStartInfo) func(trace.DriverConnPoolReleaseDoneInfo) {
							releases.Add(1)

							return nil
						},
					}),
				)
				require.NoError(t, err)

				defer func() { require.NoError(t, driver.Close(context.Background())) }()

				parent := driver
				for range depth {
					parent, err = parent.With(context.Background())
					require.NoError(t, err)
				}

				var childContext context.Context

				child, err := parent.With(context.Background(),
					WithBalancer(balancers.RandomChoice()),
					WithDiscoveryInterval(-1),
					func(ctx context.Context, child *Driver) error {
						childContext = ctx
						if failOption {
							return status.Error(codes.InvalidArgument, "synthetic option failure")
						}

						return nil
					},
				)
				require.Error(t, err)
				require.Nil(t, child)
				require.ErrorIs(t, childContext.Err(), context.Canceled)
				require.EqualValues(t, 1, releases.Load())

				require.NoError(t, driver.Close(context.Background()))
				require.EqualValues(t, depth+2, releases.Load())
				require.ErrorIs(t, driver.pool.AddRef(context.Background()), conn.ErrClosedPool)
			})
		}
	}
}

func TestClosingGrandchildKeepsParentRegistered(t *testing.T) {
	driver, err := Open(t.Context(), "grpc://localhost:2135/missing",
		WithAnonymousCredentials(), WithBalancer(balancers.SingleConn()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(context.Background())) })

	var childContext context.Context
	child, err := driver.With(t.Context(), func(ctx context.Context, _ *Driver) error {
		childContext = ctx

		return nil
	})
	require.NoError(t, err)
	parentContext := childContext

	grandchild, err := child.With(t.Context())
	require.NoError(t, err)
	require.NoError(t, grandchild.Close(t.Context()))
	require.NoError(t, parentContext.Err())

	require.NoError(t, driver.Close(t.Context()))
	require.ErrorIs(t, parentContext.Err(), context.Canceled)
	require.ErrorIs(t, driver.pool.AddRef(t.Context()), conn.ErrClosedPool)
}
