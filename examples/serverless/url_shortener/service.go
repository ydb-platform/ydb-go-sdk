package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbMetrics "github.com/ydb-platform/ydb-go-sdk-prometheus"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//go:embed static/index.html
var static embed.FS

var (
	short = regexp.MustCompile(`[a-zA-Z0-9]{8}`)
	long  = regexp.MustCompile(`https?://(?:[-\w.]|%[\da-fA-F]{2})+`)
)

func hash(s string) (string, error) {
	hasher := fnv.New32a()
	_, err := hasher.Write([]byte(s))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func isShortCorrect(link string) bool {
	return short.FindStringIndex(link) != nil
}

func isLongCorrect(link string) bool {
	return long.FindStringIndex(link) != nil
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
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

var once sync.Once

func getService(ctx context.Context, dsn string, opts ...ydb.Option) (s *service, err error) {
	once.Do(func() {
		var (
			registry = prometheus.NewRegistry()
			calls    = prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: "app",
				Name:      "calls",
			}, []string{
				"method",
				"success",
			})
			callsLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: "app",
				Name:      "latency",
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
				ydbMetrics.WithDetails(
					trace.DetailsAll,
				),
			),
		)

		s = &service{
			registry: registry,
			router:   mux.NewRouter(),

			calls:        calls,
			callsLatency: callsLatency,
			callsErrors:  callsErrors,
		}

		s.db, err = ydb.Open(ctx, dsn, opts...)
		if err != nil {
			err = fmt.Errorf("connect error: %w", err)
			return
		}

		s.router.Handle("/metrics", promhttp.InstrumentMetricHandler(
			registry, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		))
		s.router.HandleFunc("/", s.handleIndex).Methods(http.MethodGet)
		s.router.HandleFunc("/shorten", s.handleShorten).Methods(http.MethodPost)
		s.router.HandleFunc("/{[0-9a-fA-F]{8}}", s.handleLonger).Methods(http.MethodGet)

		err = s.createTable(ctx)
		if err != nil {
			_ = s.db.Close(ctx)
			err = fmt.Errorf("error on create table: %w", err)
			return
		}
	})
	if err != nil {
		once = sync.Once{}
		return nil, err
	}
	return s, nil
}

func (s *service) Close(ctx context.Context) {
	_ = s.db.Close(ctx)
}

func (s *service) createTable(ctx context.Context) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			CREATE TABLE urls (
				src Text,
				hash Text,

				PRIMARY KEY (hash)
			);
		`)),
		templateConfig{
			TablePathPrefix: path.Join(s.db.Name(), prefix),
		},
	)
	return s.db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			err := s.ExecuteSchemeQuery(ctx, query)
			return err
		},
	)
}

func (s *service) insertShort(ctx context.Context, url string) (h string, err error) {
	h, err = hash(url)
	if err != nil {
		return "", err
	}
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $hash as Text;
			DECLARE $src as Text;

			REPLACE INTO
				urls (hash, src)
			VALUES
				($hash, $src);
		`)),
		templateConfig{
			TablePathPrefix: path.Join(s.db.Name(), prefix),
		},
	)
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	err = s.db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				table.NewQueryParameters(
					table.ValueParam("$hash", types.TextValue(h)),
					table.ValueParam("$src", types.TextValue(url)),
				),
				options.WithCollectStatsModeBasic(),
			)
			return
		},
	)
	return h, err
}

func (s *service) selectLong(ctx context.Context, hash string) (url string, err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $hash as Text;

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
	readTx := table.TxControl(
		table.BeginTx(
			table.WithSnapshotReadOnly(),
		),
		table.CommitTx(),
	)
	var res result.Result
	err = s.db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$hash", types.TextValue(hash)),
				),
				options.WithCollectStatsModeBasic(),
			)
			return err
		},
	)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = res.Close()
	}()
	var src string
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			err = res.ScanNamed(
				named.OptionalWithDefault("src", &src),
			)
			return src, err
		}
	}
	return "", fmt.Errorf("hash '%s' is not found", hash)
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
	data := map[string]interface{}{
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
	path := strings.Split(r.URL.Path, "/")
	if !isShortCorrect(path[len(path)-1]) {
		err = fmt.Errorf("'%s' is not a valid short path", path[len(path)-1])
		writeResponse(w, http.StatusBadRequest, err.Error())
		return
	}
	url, err = s.selectLong(r.Context(), path[len(path)-1])
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	http.Redirect(w, r, url, http.StatusSeeOther)
}

// Serverless is an entrypoint for serverless yandex function
//
//nolint:deadcode
func Serverless(w http.ResponseWriter, r *http.Request) {
	s, err := getService(
		r.Context(),
		os.Getenv("YDB"),
		environ.WithEnvironCredentials(r.Context()),
	)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer s.Close(r.Context())
	s.router.ServeHTTP(w, r)
}
