package ydb

import (
	"crypto/sha256"
	"crypto/x509"
	"os"
	"path/filepath"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var DisableFileCache = false

var fileCache sync.Map

// readFileCached is a cached version of os.ReadFile. Cache key is
//  filepath.Clean(file)
func readFileCached(file string) ([]byte, error) {
	file = filepath.Clean(file)
	if !DisableFileCache {
		value, exists := fileCache.Load(file)
		if exists {
			bytes, ok := value.([]byte)
			if !ok {
				panic("unknown file cache type")
			}
			return bytes, nil
		}
	}
	bytes, err := os.ReadFile(file)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if !DisableFileCache {
		fileCache.Store(file, bytes)
	}
	return bytes, nil
}

var DisableCertificateCache = false

var certificateCache sync.Map

// parseCertificateCached is a cached version of x509.ParseCertificate. Cache key is
//  sha256.Sum224(der)
func parseCertificateCached(der []byte) (*x509.Certificate, error) {
	// using Sum224 for cert data similarly to tls package
	var sum [sha256.Size224]byte
	if !DisableCertificateCache {
		sum = sha256.Sum224(der)
		value, exists := certificateCache.Load(sum)
		if exists {
			cert, ok := value.(*x509.Certificate)
			if !ok {
				panic("unknown certificate cache type")
			}
			return cert, nil
		}
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if !DisableCertificateCache {
		certificateCache.Store(sum, cert)
	}
	return cert, nil
}
