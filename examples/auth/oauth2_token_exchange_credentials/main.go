package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/golang-jwt/jwt/v4"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

var (
	dsn            string
	tokenEndpoint  string
	keyID          string
	privateKeyFile string
	audience       string
	issuer         string
	subject        string
)

func init() { //nolint:gochecknoinits
	required := []string{"ydb", "private-key-file", "key-id", "token-endpoint"}
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}
	flagSet.StringVar(&dsn,
		"ydb", "",
		"YDB connection string",
	)
	flagSet.StringVar(&tokenEndpoint,
		"token-endpoint", "",
		"oauth 2.0 token exchange endpoint",
	)
	flagSet.StringVar(&keyID,
		"key-id", "",
		"key id for jwt token",
	)
	flagSet.StringVar(&privateKeyFile,
		"private-key-file", "",
		"RSA private key file for jwt token in pem format",
	)
	flagSet.StringVar(&audience,
		"audience", "",
		"audience",
	)
	flagSet.StringVar(&issuer,
		"issuer", "",
		"jwt token issuer",
	)
	flagSet.StringVar(&subject,
		"subject", "",
		"jwt token subject",
	)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		flagSet.Usage()
		os.Exit(1)
	}
	flagSet.Visit(func(f *flag.Flag) {
		for i, arg := range required {
			if arg == f.Name {
				required = append(required[:i], required[i+1:]...)
			}
		}
	})
	if len(required) > 0 {
		fmt.Printf("\nSome required options not defined: %v\n\n", required)
		flagSet.Usage()
		os.Exit(1)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := ydb.Open(ctx, dsn,
		ydb.WithOauth2TokenExchangeCredentials(
			credentials.WithTokenEndpoint(tokenEndpoint),
			credentials.WithAudience(audience),
			credentials.WithJWTSubjectToken(
				credentials.WithSigningMethod(jwt.SigningMethodRS256),
				credentials.WithKeyID(keyID),
				credentials.WithRSAPrivateKeyPEMFile(privateKeyFile),
				credentials.WithIssuer(issuer),
				credentials.WithSubject(subject),
				credentials.WithAudience(audience),
			),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close(ctx) }()

	whoAmI, err := db.Discovery().WhoAmI(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println(whoAmI.String())
}
