package tests

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	grpcCredentials "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

func TestStaticCredentials(t *testing.T) {
	t.Skip("wait for newest cr.yandex/yc/yandex-docker-local-ydb:latest was published")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var dsn string
	if v, has := os.LookupEnv("YDB_CONNECTION_STRING"); !has {
		t.Fatal("env YDB_CONNECTION_STRING required")
	} else {
		dsn = v
	}

	url, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}

	staticCredentials := credentials.NewStaticCredentials("root", "", url.Host, func() grpc.DialOption {
		if url.Scheme == "grpcs" {
			transportCredentials, transportCredentialsErr := grpcCredentials.NewClientTLSFromFile(
				os.Getenv("YDB_SSL_ROOT_CERTIFICATES_FILE"), url.Hostname(),
			)
			if err != nil {
				t.Fatalf("cannot create transport credentials: %v", transportCredentialsErr)
			}
			return grpc.WithTransportCredentials(transportCredentials)
		}
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}())

	token, err := staticCredentials.Token(ctx)
	if err != nil {
		t.Fatalf("get token failed: %v", err)
	} else {
		fmt.Printf("token: %s\n", token)
	}

	db, err := ydb.Open(
		ctx,
		"", // corner case for check replacement of endpoint+database+secure
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithCredentials(staticCredentials),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// cleanup connection
		if e := db.Close(ctx); e != nil {
			t.Fatalf("close failed: %+v", e)
		}
	}()
	_, err = db.Discovery().WhoAmI(ctx)
	if err != nil {
		t.Fatal(err)
	}
}
