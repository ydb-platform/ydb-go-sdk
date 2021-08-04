package main

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/example/internal/cli"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"github.com/yandex-cloud/ydb-go-sdk/v2/ydbx"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/go-cmd/cmd"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"text/template"
	"time"
)

type healthCheck struct {
	db       *ydbx.Client
	database string
	client   *http.Client
}

func NewHealthcheck(ctx context.Context, endpoint string, database string, secure bool) (h *healthCheck, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	db, err := ydbx.NewClient(
		ctx,
		ydbx.EndpointDatabase(
			endpoint,
			database,
			secure,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("error on create YDB client: %w", err)
	}
	h = &healthCheck{
		db:       db,
		database: database,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
			Timeout: time.Second * 10,
		},
	}
	err = h.createTable(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error on create table: %w", err)
	}
	return h, nil
}

func (h *healthCheck) Close() {
	h.db.Close()
}

func (h *healthCheck) createTable(ctx context.Context) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			CREATE TABLE healthchecks (
				url         Utf8,
				code        Int32,
				ts          DateTime,
				error       Utf8,

				PRIMARY KEY (url, ts)
			);
		`)),
		templateConfig{
			TablePathPrefix: h.database,
		},
	)
	return table.Retry(ctx, h.db.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			err := s.ExecuteSchemeQuery(ctx, query)
			return err
		}),
	)
}

func (h *healthCheck) ping(path string) result {
	url, err := url.Parse(path)
	if err != nil {
		return result{
			code: -1,
			err:  err.Error(),
		}
	}
	if url.Scheme == "" {
		url.Scheme = "http"
	}
	request, err := http.NewRequest(http.MethodGet, url.String(), nil)
	if err != nil {
		return result{
			code: -1,
			err:  err.Error(),
		}
	}
	response, err := h.client.Do(request)
	if err != nil {
		return result{
			code: -1,
			err:  err.Error(),
		}
	}
	return result{
		code: response.StatusCode,
	}
}

func (h *healthCheck) check(ctx context.Context, urls []string) (err error) {
	if len(urls) == 0 {
		return nil
	}
	codes := &sync.Map{}
	wg := &sync.WaitGroup{}
	wg.Add(len(urls))
	for _, url := range urls {
		go func(url string) {
			defer wg.Done()
			codes.Store(url, h.ping(url))
		}(url)
	}
	wg.Wait()

	return h.saveCodes(ctx, codes)
}

func (h *healthCheck) saveCodes(ctx context.Context, codes *sync.Map) (err error) {
	query := fmt.Sprintf(`
        PRAGMA TablePathPrefix("%s");

		DECLARE $url AS Utf8;
        DECLARE $code AS Int32;
        DECLARE $ts AS DateTime;
        DECLARE $error AS Utf8;

        UPSERT INTO healthchecks ( url, code, ts, error )
        VALUES ($url, $code, $ts, $error);`,
		h.database,
	)
	errs := make(chan error)
	go func() {
		codes.Range(
			func(url, code interface{}) bool {
				result := code.(result)
				writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())
				err := table.Retry(
					ctx,
					h.db.Table().Pool(),
					table.OperationFunc(
						func(ctx context.Context, s *table.Session) (err error) {
							_, _, err = s.Execute(
								ctx,
								writeTx,
								query,
								table.NewQueryParameters(
									table.ValueParam("$url", ydb.UTF8Value(url.(string))),
									table.ValueParam("$code", ydb.Int32Value(int32(result.code))),
									table.ValueParam("$ts", ydb.DatetimeValue(ydb.Time(time.Now()).Datetime())),
									table.ValueParam("$error", ydb.UTF8Value(result.err)),
								),
							)
							return err
						},
					),
				)
				if err != nil {
					fmt.Println("error on save code", url, result, err)
					errs <- err
				}
				return true
			},
		)
		close(errs)
	}()
	ee := make([]string, 0)
	for err := range errs {
		ee = append(ee, err.Error())
	}
	if len(ee) == 0 {
		return nil
	}
	return fmt.Errorf("errors: [%s]", strings.Join(ee, ","))
}

// Check is a entrypoint for serverless yandex function
func Check(ctx context.Context) error {
	return (&Command{}).run(
		ctx,
		os.Getenv("YDB_ENDPOINT"),
		os.Getenv("YDB_DATABASE"),
		os.Getenv("YDB_TABLE_PREFIX"),
		strings.Split(os.Getenv("HEALTHCHECK_URLS"), ","),
	)
}
