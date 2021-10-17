package credentials

import (
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os/user"
	"path/filepath"
)

func AppendCertsFromFile(certPool *x509.CertPool, caFile string) error {
	caFile, err := func(path string) (string, error) {
		if len(path) == 0 || path[0] != '~' {
			return path, nil
		}
		usr, err := user.Current()
		if err != nil {
			return "", err
		}
		return filepath.Join(usr.HomeDir, path[1:]), nil
	}(caFile)
	if err != nil {
		return err
	}
	pem, err := ioutil.ReadFile(caFile)
	if err != nil {
		return err
	}
	if ok := certPool.AppendCertsFromPEM(pem); !ok {
		return fmt.Errorf("certificates file '%s' can not be append: %w", caFile, err)
	}
	return nil
}
