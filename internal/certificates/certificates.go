package certificates

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	// fileCache
	//  map[string][]*x509.Certificate
	fileCache sync.Map
	// FileCacheEnabled turns caching in ParseCertificatesFromFile on or off.
	// This varbialbe MUST not be set concurrently with calls to ParseCertificatesFromFile.
	FileCacheEnabled = true
	// FileCacheHook (if not nil) is called on every call to ParseCertificatesFromFile
	// if FileCacheEnabled = true. Its argument tells whether there was a cache hit.
	// This varbialbe MUST not be set concurrently with calls to ParseCertificatesFromFile.
	FileCacheHook func(isHit bool)
)

// ParseCertificatesFromFile reads and parses pem-encoded certificate(s) from file.
func ParseCertificatesFromFile(file string) ([]*x509.Certificate, error) {
	if !FileCacheEnabled {
		certs, err := parseCertificatesFromFile(file)
		return certs, xerrors.WithStackTrace(err)
	}

	value, exists := fileCache.Load(file)
	if FileCacheHook != nil {
		FileCacheHook(exists)
	}
	if exists {
		certs, ok := value.([]*x509.Certificate)
		if !ok {
			panic("unknown file cache type")
		}
		return certs, nil
	}
	certs, err := parseCertificatesFromFile(file)
	if err == nil {
		fileCache.Store(file, certs)
	}
	return certs, xerrors.WithStackTrace(err)
}

func parseCertificatesFromFile(file string) ([]*x509.Certificate, error) {
	bytes, err := os.ReadFile(file)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	certs, err := ParseCertificatesFromPem(bytes)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return certs, nil
}

var (
	// pemCache
	//  map[string]*x509.Certificate
	pemCache sync.Map
	// PemCacheEnabled turns caching in ParseCertificate on or off.
	// This varbialbe MUST not be set concurrently with calls to ParseCertificate.
	PemCacheEnabled = true
	// PemCacheHook (if not nil) is called on every call to ParseCertificate
	// if DerCacheEnabled = true. Its argument tells whether there was a cache hit.
	// This varbialbe MUST not be set concurrently with calls to ParseCertificate.
	PemCacheHook func(isHit bool)
)

// ParseCertificate is a cached version of x509.ParseCertificate. Cache key is
//  string(der)
func ParseCertificate(der []byte) (*x509.Certificate, error) {
	if !PemCacheEnabled {
		cert, err := x509.ParseCertificate(der)
		return cert, xerrors.WithStackTrace(err)
	}
	key := string(der)

	value, exists := pemCache.Load(key)
	if PemCacheHook != nil {
		PemCacheHook(exists)
	}
	if exists {
		cert, ok := value.(*x509.Certificate)
		if !ok {
			panic("unknown file cache type")
		}
		return cert, nil
	}
	cert, err := x509.ParseCertificate(der)
	if err == nil {
		pemCache.Store(key, cert)
	}
	return cert, xerrors.WithStackTrace(err)
}

// ParseCertificatesFromPem parses one or more certificate from pem blocks in bytes.
// It returns nil error if at least one certificate was successfully parsed.
// This function uses cached ParseCertificate.
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
