package main

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/v2/ydbx"
)

const (
	invalidHashError = "'%s' is not a valid short path."
	hashNotFound     = "'%s' path is not found"
	invalidURLError  = "'%s' is not a valid URL."
)

var (
	indexPageContent = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>shortener</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
    <script src="http://code.jquery.com/jquery-3.5.0.min.js" integrity="sha256-xNzN2a4ltkB44Mc/Jz3pT4iU1cmeR0FkXs4pru/JxaQ=" crossorigin="anonymous"></script>
    <style>
        body {
            margin-top: 30px;
        }
        .shorten {
            color: blue;
        }
    </style>
</head>
<body>

<div class="container">
    <div class="row-12">
        <form id="source-url-form">
            <div class="form-row">
                <div class="col-12">
                    <input id="source" type="text" class="form-control" placeholder="https://">
                </div>
            </div>
        </form>
    </div>
    <div class="row-12">
        <a id="shorten" class="shorten" href=""/>
    </div>
</div>

<script>
    $(function() {
        let $form = $("#source-url-form");
        let $source = $("#source");
        let $shorten = $("#shorten");

        $form.submit(function(e) {
            e.preventDefault();

            $.ajax({
                url: 'url?url=' + $source.val(),
                contentType: "application/text; charset=utf-8",
                traditional: true,
                success: function(data) {
                    $shorten.html(data);
					$shorten.attr('href', data);
                },
                error: function(data) {
                    $shorten.html('invalid url')
					$shorten.attr('href', '');
                }
            });
        });
    });
</script>

</body>
</html>`

	short = regexp.MustCompile("[a-zA-Z0-9]")
	long  = regexp.MustCompile("https?://(?:[-\\w.]|(?:%[\\da-fA-F]{2}))+")
)

type urlShortener struct {
	database string
	db       *ydbx.Client
}

func NewURLShortener(ctx context.Context, endpoint string, database string, tls bool) (h *urlShortener, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	db, err := ydbx.NewClient(
		ctx,
		ydbx.EndpointDatabase(
			endpoint,
			database,
			tls,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("error on create YDB client: %w", err)
	}
	h = &urlShortener{
		database: database,
		db:       db,
	}
	err = h.createTable(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error on create table: %w", err)
	}
	return h, nil
}

func (h *urlShortener) Close() {
	h.db.Close()
}

func (h *urlShortener) createTable(ctx context.Context) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			CREATE TABLE urls (
				src Utf8,
				hash Utf8,

				PRIMARY KEY (hash)
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

func (h *urlShortener) insertShort(ctx context.Context, url string) (hash string, err error) {
	hash = Hash(url)
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $hash as Utf8;
			DECLARE $src as Utf8;

			REPLACE INTO
				urls (hash, src)
			VALUES
				($hash, $src);
		`)),
		templateConfig{
			TablePathPrefix: h.database,
		},
	)
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	return hash, table.Retry(ctx, h.db.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				table.NewQueryParameters(
					table.ValueParam("$hash", ydb.UTF8Value(hash)),
					table.ValueParam("$src", ydb.UTF8Value(url)),
				),
				table.WithCollectStatsModeBasic(),
			)
			return
		}),
	)
}

func (h *urlShortener) selectLong(ctx context.Context, hash string) (url string, err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $hash as Utf8;

			SELECT
				src
			FROM
				urls
			WHERE
				hash = $hash;
		`)),
		templateConfig{
			TablePathPrefix: h.database,
		},
	)
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)
	var res *table.Result
	err = table.Retry(ctx, h.db.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$hash", ydb.UTF8Value(hash)),
				),
				table.WithQueryCachePolicy(
					table.WithQueryCachePolicyKeepInCache(),
				),
				table.WithCollectStatsModeBasic(),
			)
			return
		}),
	)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = res.Close()
	}()
	for res.NextSet() {
		for res.NextRow() {
			if res.SeekItem("src") {
				return res.OUTF8(), nil
			}
		}
	}
	return "", fmt.Errorf(hashNotFound, hash)
}

func (h *urlShortener) Handle(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimLeft(r.URL.Path, "/")
	switch {
	case path == "":
		http.ServeContent(w, r, "", time.Now(), bytes.NewReader([]byte(indexPageContent)))
	case path == "url":
		url := r.URL.Query().Get("url")
		if !isLongCorrect(url) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf(invalidURLError, url)))
			return
		}
		hash, err := h.insertShort(r.Context(), url)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/text")
		protocol := func() string {
			if r.TLS == nil {
				return "http://"
			}
			return "https://"
		}()
		w.Write([]byte(protocol + r.Host + "/" + hash))
	default:
		if !isShortCorrect(path) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf(invalidHashError, path)))
			return
		}
		url, err := h.selectLong(r.Context(), path)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		http.Redirect(w, r, url, http.StatusSeeOther)
	}
}

func Handle(w http.ResponseWriter, r *http.Request) {
	h, err := NewURLShortener(
		r.Context(),
		os.Getenv("YDB_ENDPOINT"),
		os.Getenv("YDB_DATABASE"),
		true,
	)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	h.Handle(w, r)
}
