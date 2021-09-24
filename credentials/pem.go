package credentials

import (
	"crypto/x509"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pem"
)

func AppendCertsFromFile(certPool *x509.CertPool, caFile string) error {
	return pem.AppendCertsFromFile(certPool, caFile)
}
