package sugar

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// LoadCertificatesFromFile read and parse caFile and returns certificates
func LoadCertificatesFromFile(caFile string) ([]*x509.Certificate, error) {
	bytes, err := os.ReadFile(filepath.Clean(caFile))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return LoadCertificatesFromPem(bytes), nil
}

// LoadCertificatesFromPem parse bytes and returns certificates
func LoadCertificatesFromPem(bytes []byte) (certs []*x509.Certificate) {
	var (
		cert *x509.Certificate
		err  error
	)
	for len(bytes) > 0 {
		var block *pem.Block
		block, bytes = pem.Decode(bytes)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}
		certBytes := block.Bytes
		cert, err = x509.ParseCertificate(certBytes)
		if err != nil {
			continue
		}
		certs = append(certs, cert)
	}

	return
}
