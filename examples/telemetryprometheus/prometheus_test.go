package telemetryprometheus

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

func TestMeterScrape(t *testing.T) {
	ctx := t.Context()
	registry := prometheus.NewPedanticRegistry()
	var workers atomic.Int64
	workers.Store(3)
	callback := func(ctx context.Context, observe func(int64, ...telemetry.Attribute)) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		observe(workers.Load(), telemetry.Attribute{Key: "reader.name", Value: "parent"})

		return nil
	}
	unregister, err := Meter(ctx, registry)(telemetry.Descriptor{
		Name: "application.workers", Description: "Current workers.",
	}, callback)
	checkError(t, err)
	t.Cleanup(func() { checkError(t, unregister()) })
	server := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	t.Cleanup(server.Close)

	for _, value := range []int64{0, 3, 7} {
		workers.Store(value)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		checkError(t, err)
		response, err := server.Client().Do(request)
		checkError(t, err)
		body, err := io.ReadAll(response.Body)
		checkError(t, response.Body.Close())
		checkError(t, err)
		checkEqual(t, http.StatusOK, response.StatusCode)
		var parser expfmt.TextParser
		families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
		checkError(t, err)
		checkEqual(t, 1, len(families["application_workers"].GetMetric()))
		metric := families["application_workers"].GetMetric()[0]
		checkEqual(t, float64(value), metric.GetGauge().GetValue())
		checkEqual(t, "reader_name", metric.GetLabel()[0].GetName())
		checkEqual(t, "parent", metric.GetLabel()[0].GetValue())
	}

	checkError(t, unregister())
	checkError(t, unregister())
	families, err := registry.Gather()
	checkError(t, err)
	checkEqual(t, 0, len(families))
}

func TestMeterIndependentResources(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	meter := Meter(t.Context(), registry)
	var unregisters []func() error
	for _, name := range []string{"parent", "child"} {
		unregister, err := meter(telemetry.Descriptor{Name: "sessions", Description: "Sessions."},
			func(_ context.Context, observe func(int64, ...telemetry.Attribute)) error {
				observe(1, telemetry.Attribute{Key: "reader.name", Value: name},
					telemetry.Attribute{Key: "topic", Value: "/local/topic"})

				return nil
			})
		checkError(t, err)
		unregisters = append(unregisters, unregister)
		t.Cleanup(func() { checkError(t, unregister()) })
	}
	checkError(t, unregisters[1]())
	families, err := registry.Gather()
	checkError(t, err)
	checkEqual(t, 1, len(families))
	checkEqual(t, 1, len(families[0].GetMetric()))
	checkEqual(t, "parent", families[0].GetMetric()[0].GetLabel()[0].GetValue())
}

func TestMeterErrors(t *testing.T) {
	t.Run("initial callback", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		wantErr := errors.New("initial observation failed")
		unregister, err := Meter(t.Context(), registry)(telemetry.Descriptor{Name: "workers"},
			func(context.Context, func(int64, ...telemetry.Attribute)) error { return wantErr })
		if !errors.Is(err, wantErr) || unregister != nil {
			t.Fatalf("initial callback error not propagated: %v", err)
		}
		families, err := registry.Gather()
		checkError(t, err)
		checkEqual(t, 0, len(families))
	})
	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		unregister, err := Meter(ctx, prometheus.NewRegistry())(telemetry.Descriptor{Name: "workers"},
			func(ctx context.Context, _ func(int64, ...telemetry.Attribute)) error { return ctx.Err() })
		if !errors.Is(err, context.Canceled) || unregister != nil {
			t.Fatalf("cancellation not propagated: %v", err)
		}
	})
	t.Run("no observations", func(t *testing.T) {
		unregister, err := Meter(t.Context(), prometheus.NewRegistry())(telemetry.Descriptor{Name: "workers"},
			func(context.Context, func(int64, ...telemetry.Attribute)) error { return nil })
		if !errors.Is(err, errEmptyAttributes) || unregister != nil {
			t.Fatalf("empty attribute sets not rejected: %v", err)
		}
	})
	t.Run("registration", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		unregister, err := Meter(t.Context(), registry)(telemetry.Descriptor{Name: ""},
			func(_ context.Context, observe func(int64, ...telemetry.Attribute)) error {
				observe(1)

				return nil
			})
		if err == nil || unregister != nil {
			t.Fatal("invalid registration succeeded")
		}
		families, err := registry.Gather()
		checkError(t, err)
		checkEqual(t, 0, len(families))
	})
	t.Run("callback", func(t *testing.T) {
		registry := prometheus.NewPedanticRegistry()
		wantErr := errors.New("observation failed")
		var failed atomic.Bool
		unregister, err := Meter(t.Context(), registry)(telemetry.Descriptor{Name: "workers"},
			func(_ context.Context, observe func(int64, ...telemetry.Attribute)) error {
				if failed.Load() {
					return wantErr
				}
				observe(3)

				return nil
			})
		checkError(t, err)
		t.Cleanup(func() { checkError(t, unregister()) })
		failed.Store(true)
		_, err = registry.Gather()
		if err == nil || !strings.Contains(err.Error(), wantErr.Error()) {
			t.Fatalf("callback error not propagated: %v", err)
		}
		checkError(t, unregister())
		families, err := registry.Gather()
		checkError(t, err)
		checkEqual(t, 0, len(families))
	})
}

func TestMeterUnregisterWaitsForCollection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	registry := &capturingRegisterer{Registerer: prometheus.NewRegistry()}
	var collecting atomic.Bool
	entered := make(chan struct{})
	release := make(chan struct{})
	unregister, err := Meter(ctx, registry)(telemetry.Descriptor{Name: "workers"},
		func(ctx context.Context, observe func(int64, ...telemetry.Attribute)) error {
			if collecting.Load() {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			observe(1)

			return nil
		})
	checkError(t, err)
	releaseCallback := sync.OnceFunc(func() { close(release) })
	defer releaseCallback()
	collecting.Store(true)
	gathered := make(chan error, 1)
	go func() {
		_, err := registry.Registerer.(prometheus.Gatherer).Gather()
		gathered <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The callback holds the same lock that unregister needs, independent of
	// whether the unregister goroutine has been scheduled yet.
	c := registry.registered.(*collector)
	if c.mu.TryLock() {
		c.mu.Unlock()
		t.Fatal("collection did not hold the teardown lock")
	}
	unregistered := make(chan error, 1)
	go func() { unregistered <- unregister() }()
	releaseCallback()
	for _, done := range []<-chan error{gathered, unregistered} {
		select {
		case err := <-done:
			checkError(t, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	families, err := registry.Registerer.(prometheus.Gatherer).Gather()
	checkError(t, err)
	checkEqual(t, 0, len(families))
	// A native Gather may already have copied the collector before unregister.
	// Its delayed Collect must not invoke the now-closed resource callback.
	stale := make(chan prometheus.Metric, 1)
	c.Collect(stale)
	checkEqual(t, 0, len(stale))
}

func checkError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func checkEqual[T comparable](t *testing.T, want, got T) {
	t.Helper()
	if want != got {
		t.Fatalf("want %v, got %v", want, got)
	}
}

type capturingRegisterer struct {
	prometheus.Registerer

	registered prometheus.Collector
}

func (r *capturingRegisterer) Register(c prometheus.Collector) error {
	r.registered = c

	return r.Registerer.Register(c)
}
