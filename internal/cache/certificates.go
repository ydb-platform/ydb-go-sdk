package cache

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var fileCache = newCache()

func FileCache() Cache {
	return fileCache
}

// ParseCertificatesFromFile reads and parses pem-encoded certificate(s) from file.
// The cache key is clean, absolute filepath with all symlinks evaluated.
func ParseCertificatesFromFile(file string) ([]*x509.Certificate, error) {
	var err error
	file, err = filepath.Abs(file)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	file, err = filepath.EvalSymlinks(file)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	value, err := fileCache.loadOrStore(nopLazyValue(file), func() (interface{}, error) {
		bytes, err2 := os.ReadFile(file)
		if err2 != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		return ParseCertificatesFromPem(bytes)
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	certs, ok := value.([]*x509.Certificate)
	if !ok {
		panic("unknown file cache type")
	}
	return certs, nil
}

var certificateCache = newCache()

func CertificateCache() Cache {
	return certificateCache
}

// ParseCertificate is a cached version of x509.ParseCertificate. Cache key is
//  string(der)
func ParseCertificate(der []byte) (*x509.Certificate, error) {
	key := string(der)

	value, err := certificateCache.loadOrStore(nopLazyValue(key), func() (interface{}, error) {
		return x509.ParseCertificate(der)
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	cert, ok := value.(*x509.Certificate)
	if !ok {
		panic("unknown file cache type")
	}
	return cert, nil
}

// ParseCertificatesFromPem parses one or more certificate from pem blocks in bytes.
// It returns nil error if at least one certificate was successfully parsed.
// This function uses chached ParseCertificate.
func ParseCertificatesFromPem(bytes []byte) (certs []*x509.Certificate, err error) {
	var block *pem.Block
	for len(bytes) > 0 {
		block, bytes = pem.Decode(bytes)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}
		var cert *x509.Certificate
		cert, err = ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		certs = append(certs, cert)
	}
	if len(certs) > 0 {
		return certs, nil
	}
	return nil, xerrors.WithStackTrace(err)
}
