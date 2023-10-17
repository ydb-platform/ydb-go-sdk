//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
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

func TestDatabaseSqlStaticCredentials(t *testing.T) {
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

	t.Run("sql.Open", func(t *testing.T) {
		var db *sql.DB
		db, err = sql.Open("ydb", u.String())
		require.NoError(t, err)

		err = db.PingContext(ctx)
		require.NoError(t, err)

		err = db.Close()
		require.NoError(t, err)
	})

	t.Run("sql.OpenDB", func(t *testing.T) {
		var cc *ydb.Driver
		cc, err = ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
			ydb.WithCredentials(credentials.NewStaticCredentials(u.User.Username(), func() string {
				password, _ := u.User.Password()
				return password
			}(), u.Host, credentials.WithGrpcDialOptions(func() grpc.DialOption {
				if u.Scheme == "grpcs" { //nolint:goconst
					transportCredentials, transportCredentialsErr := grpcCredentials.NewClientTLSFromFile(
						os.Getenv("YDB_SSL_ROOT_CERTIFICATES_FILE"), u.Hostname(),
					)
					if err != nil {
						t.Fatalf("cannot create transport credentials: %v", transportCredentialsErr)
					}
					return grpc.WithTransportCredentials(transportCredentials)
				}
				return grpc.WithTransportCredentials(insecure.NewCredentials())
			}()))),
		)
		require.NoError(t, err)

		defer func() {
			// cleanup
			_ = cc.Close(ctx)
		}()

		c, err := ydb.Connector(cc)
		require.NoError(t, err)

		defer func() {
			// cleanup
			_ = c.Close()
		}()

		db := sql.OpenDB(c)
		defer func() {
			// cleanup
			_ = db.Close()
		}()

		err = db.PingContext(ctx)
		require.NoError(t, err)
	})
}
