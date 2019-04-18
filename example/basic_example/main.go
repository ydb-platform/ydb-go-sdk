package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/auth/iam"
	"github.com/yandex-cloud/ydb-go-sdk/internal/traceutil"
)

func main() {
	var (
		prefix   string
		endpoint string
		trace    bool
	)
	config := new(ydb.DriverConfig)
	flag.StringVar(&config.Database, "database", "", "name of the database to use")
	flag.StringVar(&endpoint, "endpoint", "", "endpoint url to use")
	flag.StringVar(&prefix, "path", "", "tables path")
	flag.BoolVar(&trace, "trace", false, "trace driver")
	flag.Parse()

	config.Credentials = credentials()

	if trace {
		var dtrace ydb.DriverTrace
		traceutil.Stub(&dtrace, func(name string, args ...interface{}) {
			log.Printf("[driver] %s: %+v", name, traceutil.ClearContext(args))
		})
		config.Trace = dtrace
	}

	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 64<<10)
			buf = buf[:runtime.Stack(buf, false)]
			log.Fatalf("panic recovered: %v\n%s", err, buf)
		}
		os.Exit(0)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go processSignals(map[os.Signal]func(){
		syscall.SIGINT: func() {
			if ctx.Err() != nil {
				log.Fatal("forced quit")
			}
			cancel()
		},
	})

	log.SetFlags(0)

	if err := run(ctx, endpoint, prefix, config); err != nil {
		log.Fatal(err)
	}
}

func processSignals(m map[os.Signal]func()) {
	ch := make(chan os.Signal, len(m))
	for sig := range m {
		signal.Notify(ch, sig)
	}
	for sig := range ch {
		log.Printf("signal received: %s", sig)
		m[sig]()
	}
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

func credentials() ydb.Credentials {
	if token := os.Getenv("YDB_TOKEN"); token != "" {
		return ydb.AuthTokenCredentials{
			AuthToken: token,
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
