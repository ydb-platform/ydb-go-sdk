package credentials

import (
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

func AppendCertsFromFile(certPool *x509.CertPool, caFile string) error {
	pem, err := ioutil.ReadFile(caFile)
	if err != nil {
		return err
	}
	if ok := certPool.AppendCertsFromPEM(pem); !ok {
		return fmt.Errorf("certificates file '%s' can not be append: %w", caFile, err)
	}
	return nil

}
