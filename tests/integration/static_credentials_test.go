//go:build integration
// +build integration

package integration

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcCredentials "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
)

func TestStaticCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var dsn string
	if v, has := os.LookupEnv("YDB_CONNECTION_STRING"); !has {
		t.Fatal("env YDB_CONNECTION_STRING required")
	} else {
		dsn = v
	}

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}

	if version.Gte(os.Getenv("YDB_VERSION"), "23.3") {
		u.User = url.UserPassword("root", "1234")
	} else {
		u.User = url.User("root")
	}

	staticCredentials := credentials.NewStaticCredentials(u.User.Username(), func() string {
		password, _ := u.User.Password()
		return password
	}(), u.Host, credentials.WithGrpcDialOptions(func() grpc.DialOption {
		if u.Scheme == "grpcs" {
			transportCredentials, transportCredentialsErr := grpcCredentials.NewClientTLSFromFile(
				os.Getenv("YDB_SSL_ROOT_CERTIFICATES_FILE"), u.Hostname(),
			)
			if err != nil {
				t.Fatalf("cannot create transport credentials: %v", transportCredentialsErr)
			}
			return grpc.WithTransportCredentials(transportCredentials)
		}
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}()))

	token, err := staticCredentials.Token(ctx)
	require.NoError(t, err)

	t.Logf("token: %s\n", token)

	db, err := ydb.Open(ctx,
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
