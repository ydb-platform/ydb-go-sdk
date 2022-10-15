package cached

import (
	"crypto/sha256"
	"crypto/x509"
	"os"
	"path/filepath"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var fileCache = newCache()

func FileCache() Cache {
	return fileCache
}

// ReadFileCached is a cached version of os.ReadFile. Cache key is
//  filepath.Clean(file)
func ReadFileCached(file string) ([]byte, error) {
	file = filepath.Clean(file)

	value, err := fileCache.loadOrStore(nopLazyValue(file), func() (interface{}, error) {
		return os.ReadFile(file)
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	bytes, ok := value.([]byte)
	if !ok {
		panic("unknown file cache type")
	}
	return bytes, nil
}

var certificateCache = newCache()

func CertificateCache() Cache {
	return certificateCache
}

// ParseCertificateCached is a cached version of x509.ParseCertificate. Cache key is
//  sha256.Sum224(der)
func ParseCertificateCached(der []byte) (*x509.Certificate, error) {
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
