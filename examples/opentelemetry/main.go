// Package main demonstrates how to wire ydb-go-sdk's spans abstraction into
// the OpenTelemetry Go SDK so that QueryService RPCs and retry loops produce
// OTel-compliant spans (CLIENT for gRPC calls, INTERNAL for the retry
// scaffolding).
//
// The program executes a few queries inside a transaction with idempotent
// retries, generating a span tree that looks like this:
//
//	ydb.RunWithRetry         (INTERNAL)
//	└─ ydb.Try               (INTERNAL)              // 1st attempt: no extra tags
//	   ├─ ydb.ExecuteQuery   (CLIENT)
//	   ├─ ydb.ExecuteQuery   (CLIENT)
//	   └─ ydb.Commit         (CLIENT)
//
// On a retry, an additional sibling ydb.Try would carry
// `ydb.retry.backoff_ms` equal to the actual sleep that preceded it.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
)

const (
	defaultDSN         = "grpc://localhost:2136/local"
	defaultOTLPAddress = "localhost:4317"
	serviceName        = "ydb-go-sdk-otel-trace-sample"
)

func main() {
	dsn := envOrDefault("YDB_DSN", defaultDSN)
	otlpAddr := envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", defaultOTLPAddress)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tp, err := newTracerProvider(ctx, otlpAddr)
	if err != nil {
		log.Fatalf("failed to create trace provider: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}()
	otel.SetTracerProvider(tp)

	// Give the cluster a generous window to come up: under arm64/Rosetta
	// emulation `ydbplatform/local-ydb` may take 30-60s after the
	// container's healthcheck flips to `healthy` before its discovery RPCs
	// are actually responsive.
	openCtx, cancelOpen := context.WithTimeout(ctx, 2*time.Minute)
	defer cancelOpen()

	db, err := openDB(openCtx, dsn)
	if err != nil {
		log.Fatalf("failed to open YDB: %v", err)
	}
	defer func() { _ = db.Close(ctx) }()

	if err := runDemo(ctx, db); err != nil {
		log.Fatalf("demo failed: %v", err)
	}
	log.Println("demo finished, traces exported to OTLP endpoint", otlpAddr)
}

func envOrDefault(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return def
}

func newTracerProvider(ctx context.Context, otlpAddr string) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpAddr),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}
	// Use a schemaless resource for the service-specific attributes so the
	// merge with resource.Default() (whose schema URL pins to a specific
	// OTel semconv version) succeeds regardless of which semconv version
	// this module was compiled against.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", serviceName),
	))
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	), nil
}

func openDB(ctx context.Context, dsn string) (*ydb.Driver, error) {
	// Best-effort parsing of database / endpoint from the DSN so the adapter
	// can attach db.namespace / server.address / server.port.
	endpoint, database := parseDSN(dsn)
	adapter := newOTelAdapter(serviceName, database, endpoint)

	return ydb.Open(ctx, dsn,
		ydb.WithApplicationName(serviceName),
		// Bump dial timeout: the SDK default is 5s which is fine on a real
		// cluster but tight when YDB is a freshly-booting `local-ydb`
		// container (especially under Rosetta on arm64). The driver's
		// internal cluster-discovery retry loop will retry a few times
		// using this per-attempt timeout.
		ydb.WithDialTimeout(30*time.Second),
		spans.WithTraces(adapter),
	)
}

// parseDSN extracts host:port and database from the YDB connection string,
// stripping the grpc[s]:// scheme. The result is purely informational —
// ydb.Open uses the original DSN.
func parseDSN(dsn string) (endpoint, database string) {
	endpoint, database = dsn, ""
	for _, prefix := range []string{"grpcs://", "grpc://"} {
		if len(dsn) >= len(prefix) && dsn[:len(prefix)] == prefix {
			endpoint = dsn[len(prefix):]

			break
		}
	}
	if i := indexByte(endpoint, '/'); i >= 0 {
		database = endpoint[i:]
		endpoint = endpoint[:i]
	}

	return endpoint, database
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}

	return -1
}

func runDemo(ctx context.Context, db *ydb.Driver) error {
	return db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		if err := tx.Exec(ctx, `SELECT 1+1 AS sum`); err != nil {
			return err
		}
		row, err := tx.QueryRow(ctx, `SELECT 'hello, ydb' AS msg`)
		if err != nil {
			return err
		}
		var msg string
		if err := row.Scan(&msg); err != nil {
			return err
		}
		log.Println("got:", msg)

		return nil
	}, query.WithIdempotent())
}
