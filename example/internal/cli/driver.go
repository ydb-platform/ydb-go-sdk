package cli

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/auth/iam"
	"github.com/yandex-cloud/ydb-go-sdk/auth/metadata"
	"github.com/yandex-cloud/ydb-go-sdk/internal/traceutil"
)

func ExportDriverConfig(flag *flag.FlagSet) func(Parameters) *ydb.DriverConfig {
	var (
		config ydb.DriverConfig
		trace  bool
	)
	flag.BoolVar(&trace,
		"driver-trace", false,
		"trace all driver events",
	)
	flag.DurationVar(&config.DiscoveryInterval,
		"driver-discovery", 0,
		"driver's discovery interval",
	)
	return func(params Parameters) *ydb.DriverConfig {
		if trace {
			var dtrace ydb.DriverTrace
			traceutil.Stub(&dtrace, func(name string, args ...interface{}) {
				log.Printf(
					"[driver] %s: %+v",
					name, traceutil.ClearContext(args),
				)
			})
			config.Trace = dtrace
		}

		config.Database = params.Database
		config.Credentials = credentials()

		return &config
	}
}

func credentials() ydb.Credentials {
	if token := os.Getenv("YDB_TOKEN"); token != "" {
		return ydb.AuthTokenCredentials{
			AuthToken: token,
		}
	}
	if addr := os.Getenv("YDB_METADATA"); addr != "" {
		return &metadata.Client{
			Addr: addr,
		}
	}

	pk := os.Getenv("SA_PRIVATE_KEY_FILE")
	if pk != "" {
		var certPool *x509.CertPool
		if ca := os.Getenv("SSL_ROOT_CERTIFICATES_FILE"); ca != "" {
			certPool = mustReadRootCerts(ca)
		} else {
			certPool = mustReadSystemRootCerts()
		}
		return &iam.Client{
			Key:    mustReadPrivateKey(pk),
			KeyID:  mustGetenv("SA_ACCESS_KEY_ID"),
			Issuer: mustGetenv("SA_ID"),

			Endpoint: mustGetenv("SA_ENDPOINT"), // iam.api.cloud.yandex.net:443
			CertPool: certPool,
		}
	}

	return nil
}

func readFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ioutil.ReadAll(file)
}

func readPrivateKey(path string) (key *rsa.PrivateKey, err error) {
	p, err := readFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(p)
	if block == nil {
		return nil, fmt.Errorf("invalid pem encoding")
	}
	key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return
	}
	x, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
	if key, _ = x.(*rsa.PrivateKey); key != nil {
		err = nil
	}
	return
}

func mustReadPrivateKey(path string) *rsa.PrivateKey {
	key, err := readPrivateKey(path)
	if err != nil {
		panic(fmt.Errorf("read private key error: %v", err))
	}
	return key
}

func mustGetenv(name string) string {
	x := os.Getenv(name)
	if x == "" {
		panic(fmt.Sprintf("environment parameter is missing or empty: %q", name))
	}
	return x
}

func readRootCerts(path string) (*x509.CertPool, error) {
	p, err := readFile(path)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(p); !ok {
		return nil, fmt.Errorf("parse pem error")
	}
	return roots, nil
}

func mustReadRootCerts(path string) *x509.CertPool {
	roots, err := readRootCerts(path)
	if err != nil {
		panic(fmt.Errorf("read root certs error: %v", err))
	}
	return roots
}

func mustReadSystemRootCerts() *x509.CertPool {
	roots, err := x509.SystemCertPool()
	if err != nil {
		panic(fmt.Errorf("read system root certs error: %v", err))
	}
	return roots
}
