package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"regexp"
	"sync"
	"text/template"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbMetrics "github.com/ydb-platform/ydb-go-sdk-prometheus/v2"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//go:embed static/index.html
var static embed.FS

const shortHashPattern = `[a-fA-F0-9]{8}(?:[a-fA-F0-9]{24})?`

var (
	short = regexp.MustCompile(`^` + shortHashPattern + `$`)
	long  = regexp.MustCompile(`https?://(?:[-\w.]|%[\da-fA-F]{2})+`)
)

func hash(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:16])
}

func isShortCorrect(link string) bool {
	return short.FindStringIndex(link) != nil
}

func isLongCorrect(link string) bool {
	return long.FindStringIndex(link) != nil
}

func render(t *template.Template, data any) string {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		panic(err)
	}

	return buf.String()
}

type templateConfig struct {
	TablePathPrefix string
}

type service struct {
	db       *ydb.Driver
	registry *prometheus.Registry
	router   *mux.Router

	calls        *prometheus.GaugeVec
	callsLatency *prometheus.HistogramVec
	callsErrors  *prometheus.GaugeVec
}

type serviceCache struct {
	mu      sync.Mutex
	service *service
}

func (c *serviceCache) get(initService func() (*service, error)) (*service, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.service != nil {
		return c.service, nil
	}

	s, err := initService()
	if err != nil {
		return nil, err
	}
	c.service = s

	return s, nil
}

var services serviceCache

func getService(ctx context.Context, dsn string, opts ...ydb.Option) (*service, error) {
	return services.get(func() (*service, error) {
		var (
			registry = prometheus.NewRegistry()
			calls    = prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: "app",
				Name:      "calls",
				Help:      "application calls counter",
			}, []string{
				"method",
				"success",
			})
			callsLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: "app",
				Name:      "latency",
				Help:      "application calls latencies",
				Buckets: []float64{
					(1 * time.Millisecond).Seconds(),
					(5 * time.Millisecond).Seconds(),
					(10 * time.Millisecond).Seconds(),
					(50 * time.Millisecond).Seconds(),
					(100 * time.Millisecond).Seconds(),
					(500 * time.Millisecond).Seconds(),
					(1000 * time.Millisecond).Seconds(),
					(5000 * time.Millisecond).Seconds(),
					(10000 * time.Millisecond).Seconds(),
				},
			}, []string{
				"success",
				"method",
			})
			callsErrors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: "app",
				Name:      "errors",
				Help:      "application errors counter",
			}, []string{
				"method",
			})
		)

		registry.MustRegister(calls)
		registry.MustRegister(callsLatency)
		registry.MustRegister(callsErrors)

		opts = append(
			opts,
			ydbMetrics.WithTraces(
				registry,
				ydbMetrics.WithSeparator("_"),
				ydbMetrics.WithDetailer(
					trace.DetailsAll,
				),
			),
		)

		s := &service{
			registry: registry,
			router:   mux.NewRouter(),

			calls:        calls,
			callsLatency: callsLatency,
			callsErrors:  callsErrors,
		}

		var err error
		s.db, err = ydb.Open(ctx, dsn, opts...)
		if err != nil {
			return nil, fmt.Errorf("connect error: %w", err)
		}

		s.router.Handle("/metrics", promhttp.InstrumentMetricHandler(
			registry, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		))
		s.router.HandleFunc("/", s.handleIndex).Methods(http.MethodGet)
		s.router.HandleFunc("/shorten", s.handleShorten).Methods(http.MethodPost)
		s.router.HandleFunc("/{short:"+shortHashPattern+"}", s.handleLonger).Methods(http.MethodGet)

		err = s.createTable(ctx)
		if err != nil {
			_ = s.db.Close(ctx)

			return nil, fmt.Errorf("error on create table: %w", err)
		}

		return s, nil
	})
}

func (s *service) Close(ctx context.Context) {
	_ = s.db.Close(ctx)
}

func (s *service) createTable(ctx context.Context) error {
	sql := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

				CREATE TABLE IF NOT EXISTS urls (
				src Text,
				hash Text,

				PRIMARY KEY (hash)
			);
		`)),
		templateConfig{
			TablePathPrefix: path.Join(s.db.Name(), prefix),
		},
	)

	return s.db.Query().Exec(ctx, sql, query.WithIdempotent())
}

func (s *service) insertShort(ctx context.Context, url string) (h string, err error) {
	h = hash(url)
	sql := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			REPLACE INTO
				urls (hash, src)
			VALUES
				($hash, $src);
		`)),
		templateConfig{
			TablePathPrefix: path.Join(s.db.Name(), prefix),
		},
	)
	err = s.db.Query().Exec(ctx, sql,
		query.WithParameters(ydb.ParamsBuilder().
			Param("$hash").Text(h).
			Param("$src").Text(url).
			Build()),
		query.WithIdempotent(),
	)

	return h, err
}

func (s *service) selectLong(ctx context.Context, hash string) (url string, err error) {
	sql := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			SELECT
				src
			FROM
				urls
			WHERE
				hash = $hash;
		`)),
		templateConfig{
			TablePathPrefix: path.Join(s.db.Name(), prefix),
		},
	)
	err = s.db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		row, err := session.QueryRow(ctx, sql, query.WithParameters(ydb.ParamsBuilder().
			Param("$hash").Text(hash).
			Build()))
		if errors.Is(err, query.ErrNoRows) {
			return fmt.Errorf("hash '%s' is not found", hash)
		}
		if err != nil {
			return err
		}
		var attemptURL string
		if err = row.Scan(&attemptURL); err != nil {
			return err
		}
		url = attemptURL

		return nil
	}, query.WithIdempotent())
	if err != nil {
		return "", err
	}

	return url, nil
}

func writeResponse(w http.ResponseWriter, statusCode int, body string) {
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(body))
}

func successToString(b bool) string {
	if b {
		return "true"
	}

	return "false"
}

func (s *service) handleIndex(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		tpl   *template.Template
		start = time.Now()
	)
	defer func() {
		if err != nil {
			s.callsErrors.With(prometheus.Labels{
				"method": "index",
			}).Add(1)
		}
		s.callsLatency.With(prometheus.Labels{
			"method":  "index",
			"success": successToString(err == nil),
		}).Observe(time.Since(start).Seconds())
		s.calls.With(prometheus.Labels{
			"method":  "index",
			"success": successToString(err == nil),
		}).Add(1)
	}()
	tpl, err = template.ParseFS(static, "static/index.html")
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())

		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	data := map[string]any{
		"userAgent": r.UserAgent(),
	}
	if err = tpl.Execute(w, data); err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())

		return
	}
}

func (s *service) handleShorten(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		url   []byte
		hash  string
		start = time.Now()
	)
	defer func() {
		if err != nil {
			s.callsErrors.With(prometheus.Labels{
				"method": "shorten",
			}).Add(1)
		}
		s.callsLatency.With(prometheus.Labels{
			"method":  "shorten",
			"success": successToString(err == nil),
		}).Observe(time.Since(start).Seconds())
		s.calls.With(prometheus.Labels{
			"method":  "index",
			"success": successToString(err == nil),
		}).Add(1)
	}()
	url, err = io.ReadAll(r.Body)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())

		return
	}
	if !isLongCorrect(string(url)) {
		err = fmt.Errorf("'%s' is not a valid URL", url)
		writeResponse(w, http.StatusBadRequest, err.Error())

		return
	}
	hash, err = s.insertShort(r.Context(), string(url))
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())

		return
	}
	w.Header().Set("Content-Type", "application/text")
	writeResponse(w, http.StatusOK, hash)
}

func (s *service) handleLonger(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		url   string
		start = time.Now()
	)
	defer func() {
		if err != nil {
			s.callsErrors.With(prometheus.Labels{
				"method": "longer",
			}).Add(1)
		}
		s.callsLatency.With(prometheus.Labels{
			"method":  "longer",
			"success": successToString(err == nil),
		}).Observe(time.Since(start).Seconds())
		s.calls.With(prometheus.Labels{
			"method":  "index",
			"success": successToString(err == nil),
		}).Add(1)
	}()
	shortLink := mux.Vars(r)["short"]
	if !isShortCorrect(shortLink) {
		err = fmt.Errorf("'%s' is not a valid short path", shortLink)
		writeResponse(w, http.StatusBadRequest, err.Error())

		return
	}
	url, err = s.selectLong(r.Context(), shortLink)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())

		return
	}
	http.Redirect(w, r, url, http.StatusSeeOther)
}

// Serverless is an entrypoint for serverless yandex function
func Serverless(w http.ResponseWriter, r *http.Request) {
	s, err := getService(
		r.Context(),
		os.Getenv("YDB"),
		environ.WithEnvironCredentials(),
	)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())

		return
	}
	s.router.ServeHTTP(w, r)
}
