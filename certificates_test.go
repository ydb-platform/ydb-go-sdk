package ydb

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/certificates"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func BenchmarkWithCertificateCache(b *testing.B) {
	b.ReportAllocs()

	bytes, err := os.ReadFile(os.Getenv("YDB_SSL_ROOT_CERTIFICATES_FILE"))
	if err != nil {
		b.Fatal(err)
	}

	// these vars make applyOptions() do unnecessary things
	os.Unsetenv("YDB_SSL_ROOT_CERTIFICATES_FILE")
	os.Unsetenv("YDB_LOG_SEVERITY_LEVEL")

	tcs := []struct {
		name    string
		enabled bool
	}{
		{"no cache", false},
		{"cached", true},
	}
	for _, tc := range tcs {
		b.Run(tc.name, func(b *testing.B) {
			certificates.PemCacheEnabled = tc.enabled

			ctx := context.TODO()
			db, err := applyOptions(
				ctx,
				WithCertificatesFromPem(bytes),
			)
			if err != nil {
				b.Fatal(err)
			}
			// pool is needed in db.with()
			db.pool = conn.NewPool(db.config)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := db.with(
					ctx,
					WithSessionPoolSizeLimit(100),
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWithFileCache(b *testing.B) {
	b.ReportAllocs()

	caFile := os.Getenv("YDB_SSL_ROOT_CERTIFICATES_FILE")
	// these vars make applyOptions() do unnecessary things
	os.Unsetenv("YDB_SSL_ROOT_CERTIFICATES_FILE")
	os.Unsetenv("YDB_LOG_SEVERITY_LEVEL")

	tcs := []struct {
		name        string
		certEnabled bool
		fileEnabled bool
	}{
		{"no cache", false, false},
		{"cert cache", true, false},
		{"both caches", true, true},
	}
	for _, tc := range tcs {
		b.Run(tc.name, func(b *testing.B) {
			certificates.FileCacheEnabled = tc.fileEnabled
			certificates.PemCacheEnabled = tc.certEnabled

			ctx := context.TODO()
			db, err := applyOptions(
				ctx,
				WithCertificatesFromFile(caFile),
			)
			if err != nil {
				b.Fatal(err)
			}
			// pool is needed in db.with()
			db.pool = conn.NewPool(db.config)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _, err := db.with(
					ctx,
					WithSessionPoolSizeLimit(100),
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestWithCacheConcurrent(t *testing.T) {
	const nRoutines = 10

	ctx := context.TODO()
	db, err := Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
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

	wg := &sync.WaitGroup{}

	for i := 0; i < nRoutines; i++ {
		wg.Add(1)
		go func() {
			// child conns are closed on db.Close()
			_, err := db.With(
				ctx,
				WithSessionPoolSizeLimit(100),
			)
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestWithCacheHits(t *testing.T) {
	const nChildren = 10

	ctx := context.TODO()
	db, err := Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
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

	hits := 0
	certificates.FileCacheHook = func(isHit bool) {
		if isHit {
			hits++
		}
	}

	lastHits := hits
	for i := 0; i < nChildren; i++ {
		// child conns are closed on db.Close()
		_, err := db.With(
			ctx,
			WithSessionPoolSizeLimit(100),
		)
		if err != nil {
			t.Error(err)
		}
		if delta := hits - lastHits; delta < 1 {
			t.Errorf("child conn #%d: cache miss", i)
		}
		lastHits = hits
	}
}
