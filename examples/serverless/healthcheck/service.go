package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

type service struct {
	db     *ydb.Driver
	client *http.Client
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
		s := &service{
			client: &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true, //nolint:gosec
					},
				},
				Timeout: time.Second * 10,
			},
		}
		var err error
		s.db, err = ydb.Open(ctx, dsn, opts...)
		if err != nil {
			return nil, fmt.Errorf("connect error: %w", err)
		}
		err = s.createTableIfNotExists(ctx)
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

func (s *service) createTableIfNotExists(ctx context.Context) error {
	sql := fmt.Sprintf(`
			PRAGMA TablePathPrefix("%s");

			CREATE TABLE IF NOT EXISTS healthchecks (
			url         Text,
			code        Int32,
			ts          DateTime,
			error       Text,
			PRIMARY KEY (url, ts)
		) WITH (
			AUTO_PARTITIONING_BY_LOAD = ENABLED
		);`, path.Join(s.db.Name(), prefix),
	)

	return s.db.Query().Exec(ctx, sql, query.WithIdempotent())
}

func (s *service) ping(ctx context.Context, path string) (code int32, err error) {
	uri, err := url.Parse(path)
	if err != nil {
		return -1, err
	}
	if uri.Scheme == "" {
		uri.Scheme = "http"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, uri.String(), nil) //nolint:gocritic
	if err != nil {
		return -1, err
	}
	response, err := s.client.Do(request)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = response.Body.Close()
	}()

	return int32(response.StatusCode), nil
}

type row struct {
	url  string
	code int32
	err  error
}

func expandURLs(groups []string) []string {
	var urls []string
	for _, group := range groups {
		urls = append(urls, strings.Fields(group)...)
	}

	return urls
}

func (s *service) check(ctx context.Context, urls []string) error {
	targets := expandURLs(urls)
	if len(targets) == 0 {
		return nil
	}
	wg := &sync.WaitGroup{}
	rows := make([]row, len(targets))
	for idx, u := range targets {
		wg.Add(1)
		go func(idx int, u string) {
			defer wg.Done()
			out := " > '" + u + "' => "
			code, err := s.ping(ctx, u)
			if err != nil {
				fmt.Println(out + err.Error())
			} else {
				fmt.Println(out + strconv.Itoa(int(code)))
			}
			rows[idx] = row{
				url:  u,
				code: code,
				err:  err,
			}
		}(idx, u)
	}
	wg.Wait()

	return s.upsertRows(ctx, rows)
}

func (s *service) upsertRows(ctx context.Context, rows []row) (err error) {
	values := make([]types.Value, len(rows))
	for i := range rows {
		values[i] = types.StructValue(
			types.StructFieldValue("url", types.TextValue(rows[i].url)),
			types.StructFieldValue("code", types.Int32Value(rows[i].code)),
			types.StructFieldValue("ts", types.DatetimeValueFromTime(time.Now())),
			types.StructFieldValue("error", types.TextValue(func(err error) string {
				if err != nil {
					return err.Error()
				}

				return ""
			}(rows[i].err))),
		)
	}
	err = s.db.Query().Exec(ctx,
		fmt.Sprintf(`
					PRAGMA TablePathPrefix("%s");

					UPSERT INTO healthchecks ( url, code, ts, error )
					SELECT url, code, ts, error FROM AS_TABLE($rows);`,
			path.Join(s.db.Name(), prefix),
		),
		query.WithParameters(ydb.ParamsBuilder().Param("$rows").Any(types.ListValue(values...)).Build()),
		query.WithIdempotent(),
	)
	if err != nil {
		return fmt.Errorf("error on upsert rows: %w", err)
	}

	return nil
}

// Serverless is an entrypoint for serverless yandex function
func Serverless(ctx context.Context) error {
	s, err := getService(
		ctx,
		os.Getenv("YDB"),
		environ.WithEnvironCredentials(),
		ydb.WithDialTimeout(time.Second),
	)
	if err != nil {
		return fmt.Errorf("error on create service: %w", err)
	}

	return s.check(ctx, strings.Split(os.Getenv("URLS"), ","))
}
