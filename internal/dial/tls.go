package dial

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"os"
)

func Tls() (tlsConfig *tls.Config, err error) {
	tlsConfig = &tls.Config{}
	tlsConfig.RootCAs, err = x509.SystemCertPool()
	if err != nil {
		panic(err)
	}
	// append user defined certs
	if caFile, ok := os.LookupEnv("YDB_SSL_ROOT_CERTIFICATES_FILE"); ok {
		if err := credentials.AppendCertsFromFile(tlsConfig.RootCAs, caFile); err != nil {
			panic(fmt.Sprintf("Cannot load certificates from file '%s' by Env['YDB_SSL_ROOT_CERTIFICATES_FILE']: %v", caFile, err))
		}
	}
	return tlsConfig, err
}
