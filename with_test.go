package ydb //nolint:testpackage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/certificates"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

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

			for i := 0; i < n; i++ {
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
