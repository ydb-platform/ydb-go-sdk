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

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type service struct {
	db     *ydb.Driver
	client *http.Client
}

var (
	s    *service
	once sync.Once
)

func getService(ctx context.Context, dsn string, opts ...ydb.Option) (s *service, err error) {
	once.Do(func() {
		s = &service{
			client: &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
				},
				Timeout: time.Second * 10,
			},
		}
		s.db, err = ydb.Open(ctx, dsn, opts...)
		if err != nil {
			err = fmt.Errorf("connect error: %w", err)
			return
		}
		err = s.createTableIfNotExists(ctx)
		if err != nil {
			_ = s.db.Close(ctx)
			err = fmt.Errorf("error on create table: %w", err)
		}
	})
	if err != nil {
		once = sync.Once{}
		return nil, err
	}
	return s, nil
}

func (s *service) Close(ctx context.Context) {
	defer func() { _ = s.db.Close(ctx) }()
}

func (s *service) createTableIfNotExists(ctx context.Context) error {
	exists, err := sugar.IsTableExists(ctx, s.db.Scheme(), path.Join(s.db.Name(), prefix, "healthchecks"))
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");

		CREATE TABLE healthchecks (
			url         Text,
			code        Int32,
			ts          DateTime,
			error       Text,
			PRIMARY KEY (url, ts)
		) WITH (
			AUTO_PARTITIONING_BY_LOAD = ENABLED
		);`, path.Join(s.db.Name(), prefix),
	)
	return s.db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.ExecuteSchemeQuery(ctx, query)
		},
	)
}

func (s *service) ping(path string) (code int32, err error) {
	uri, err := url.Parse(path)
	if err != nil {
		return -1, err
	}
	if uri.Scheme == "" {
		uri.Scheme = "http"
	}
	request, err := http.NewRequest(http.MethodGet, uri.String(), nil)
	if err != nil {
		return -1, err
	}
	response, err := s.client.Do(request)
	if err != nil {
		return -1, err
	}
	return int32(response.StatusCode), nil
}

type row struct {
	url  string
	code int32
	err  error
}

func (s *service) check(ctx context.Context, urls []string) error {
	if len(urls) == 0 {
		return nil
	}
	wg := &sync.WaitGroup{}
	rows := make([]row, len(urls))
	for idx := range urls {
		for _, u := range strings.Split(urls[idx], " ") {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				out := " > '" + u + "' => "
				code, err := s.ping(u)
				if err != nil {
					fmt.Println(out + err.Error())
				} else {
					fmt.Println(out + strconv.Itoa(int(code)))
				}
				rows[idx] = row{
					url:  urls[idx],
					code: code,
					err:  err,
				}
			}(idx)
		}
	}
	wg.Wait()

	return s.upsertRows(ctx, rows)
}

func (s *service) upsertRows(ctx context.Context, rows []row) (err error) {
	values := make([]types.Value, len(rows))
	for i, row := range rows {
		values[i] = types.StructValue(
			types.StructFieldValue("url", types.TextValue(row.url)),
			types.StructFieldValue("code", types.Int32Value(row.code)),
			types.StructFieldValue("ts", types.DatetimeValueFromTime(time.Now())),
			types.StructFieldValue("error", types.TextValue(func(err error) string {
				if err != nil {
					return err.Error()
				}
				return ""
			}(row.err))),
		)
	}
	err = s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			_, _, err = session.Execute(ctx,
				table.SerializableReadWriteTxControl(table.CommitTx()),
				fmt.Sprintf(`
					PRAGMA TablePathPrefix("%s");
			
					DECLARE $rows AS List<Struct<
						url: Text,
						code: Int32,
						ts: DateTime,
						error: Text
					>>;

					UPSERT INTO healthchecks ( url, code, ts, error )
					SELECT url, code, ts, error FROM AS_TABLE($rows);`,
					path.Join(s.db.Name(), prefix),
				),
				table.NewQueryParameters(
					table.ValueParam("$rows", types.ListValue(values...)),
				),
			)
			return err
		},
	)
	if err != nil {
		return fmt.Errorf("error on upsert rows: %w", err)
	}
	return nil
}

// Serverless is an entrypoint for serverless yandex function
// nolint:deadcode
func Serverless(ctx context.Context) error {
	s, err := getService(
		ctx,
		os.Getenv("YDB"),
		environ.WithEnvironCredentials(ctx),
		ydb.WithDialTimeout(time.Second),
	)
	if err != nil {
		return fmt.Errorf("error on create service: %w", err)
	}
	defer s.Close(ctx)
	return s.check(ctx, strings.Split(os.Getenv("URLS"), ","))
}
