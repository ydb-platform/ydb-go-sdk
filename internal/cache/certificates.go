package cache

import (
	"crypto/sha256"
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
		var bytes []byte
		bytes, err = os.ReadFile(file)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		var (
			block *pem.Block
			certs []*x509.Certificate
		)
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
//  sha256.Sum224(der)
func ParseCertificate(der []byte) (*x509.Certificate, error) {
	// using Sum224 for cert data similarly to tls package
	var sum [sha256.Size224]byte
	lKey := func() (interface{}, error) {
		sum = sha256.Sum224(der)
		return sum, nil
	}

	value, err := certificateCache.loadOrStore(lKey, func() (interface{}, error) {
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