package ydb

import (
	"crypto/x509"
	"encoding/pem"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// loadCertificatesFromPem decodes bytes as PEM blocks and parses them into certificates.
// If at least one certificate was successfully parsed, returned error is nil.
func loadCertificatesFromPem(bytes []byte) ([]*x509.Certificate, error) {
	var (
		ok bool
		err error
		certs []*x509.Certificate
	)
	for len(bytes) > 0 {
		var block *pem.Block
		block, bytes = pem.Decode(bytes)
		if block == nil {
			break
		}
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}
		var cert *x509.Certificate
		cert, err = parseCertificateFromDer(block.Bytes)
		if err != nil {
			continue
		}
		certs = append(certs, cert)
		ok = true
	}
	if !ok {
		return certs, xerrors.WithStackTrace(err)
	}
	return certs, nil
}

var DisablePemCertificatesCache bool = false

// parseCertificateFromDer is a cached version of x509.ParseCertificate
func parseCertificateFromDer(der []byte) (*x509.Certificate, error) {
	// TODO: add certificate cache
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return cert, nil
}