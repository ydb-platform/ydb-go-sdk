package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var errNotEnthoughtFreeSeats = errors.New("not enough free seats")

type server struct {
	cache     *Cache
	mux       http.ServeMux
	db        *ydb.Driver
	dbCounter atomic.Int64
	id        int
}

func newServer(id int, db *ydb.Driver, cacheTimeout time.Duration, useCDC bool) *server {
	res := &server{
		cache: NewCache(cacheTimeout),
		db:    db,
		id:    id,
	}

	res.mux.HandleFunc("/", res.IndexPageHandler)
	res.mux.HandleFunc("/get/", res.GetFreeSeatsHandler)
	res.mux.HandleFunc("/buy/", res.BuyTicketHandler)

	if useCDC {
		go res.cdcLoop()
	}

	return res
}

func (s *server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	s.mux.ServeHTTP(writer, request)
}

func (s *server) GetFreeSeatsHandler(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	id := path.Base(request.URL.Path)

	start := time.Now()
	freeSeats, err := s.getFreeSeats(ctx, id)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}
	duration := time.Since(start)
	s.writeAnswer(writer, freeSeats, duration)
}

func (s *server) BuyTicketHandler(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	id := path.Base(request.URL.Path)

	start := time.Now()
	freeSeats, err := s.sellTicket(ctx, id)
	if err != nil {
		if errors.Is(err, errNotEnthoughtFreeSeats) {
			http.Error(writer, "Not enough free seats", http.StatusPreconditionFailed)
		} else {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}

		return
	}
	//nolint:gocritic
	// s.cache.Delete(id) // used without cdc, for single-instance application
	duration := time.Since(start)
	s.writeAnswer(writer, freeSeats, duration)
}

func (s *server) writeAnswer(writer io.Writer, freeSeats int64, duration time.Duration) {
	_, _ = fmt.Fprintf(writer, "%v\n\nDuration: %v\n", freeSeats, duration)
}

func (s *server) getFreeSeats(ctx context.Context, id string) (int64, error) {
	if content, ok := s.cache.Get(id); ok {
		return content, nil
	}

	freeSeats, err := s.getContentFromDB(ctx, id)

	if err == nil {
		s.cache.Set(id, freeSeats)
	}

	return freeSeats, err
}

func (s *server) getContentFromDB(ctx context.Context, id string) (int64, error) {
	s.dbCounter.Add(1)
	var freeSeats int64
	err := s.db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		attemptFreeSeats, err := s.getFreeSeatsTx(ctx, tx, id)
		if err != nil {
			return err
		}
		freeSeats = attemptFreeSeats

		return nil
	}, query.WithIdempotent())

	return freeSeats, err
}

func (s *server) getFreeSeatsTx(ctx context.Context, tx query.TxActor, id string) (int64, error) {
	var freeSeats int64
	row, err := tx.QueryRow(ctx, `
		SELECT freeSeats FROM bus WHERE id=$id;
`, query.WithParameters(ydb.ParamsBuilder().Param("$id").Text(id).Build()))
	if errors.Is(err, query.ErrNoRows) {
		return 0, errors.New("not found")
	}
	if err != nil {
		return 0, err
	}

	err = row.Scan(&freeSeats)
	if err != nil {
		return 0, err
	}

	return freeSeats, nil
}

func (s *server) sellTicket(ctx context.Context, id string) (int64, error) {
	var freeSeats int64
	err := s.db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		attemptFreeSeats, err := s.getFreeSeatsTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if !hasAvailableSeats(attemptFreeSeats) {
			return fmt.Errorf("failed to sell ticket: %w", errNotEnthoughtFreeSeats)
		}

		err = tx.Exec(ctx, `
UPDATE bus SET freeSeats = freeSeats - 1 WHERE id=$id;
`, query.WithParameters(ydb.ParamsBuilder().Param("$id").Text(id).Build()))
		if err != nil {
			return err
		}
		freeSeats = attemptFreeSeats - 1

		return nil
	})

	return freeSeats, err
}

func hasAvailableSeats(freeSeats int64) bool {
	return freeSeats > 0
}

func (s *server) IndexPageHandler(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()

	var busIDs []string

	err := s.db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		res, err := session.Query(ctx, "SELECT id FROM bus ORDER BY id")
		if err != nil {
			return err
		}
		defer func() {
			_ = res.Close(ctx)
		}()

		var attemptBusIDs []string
		for resultSet, err := range res.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range resultSet.Rows(ctx) {
				if err != nil {
					return err
				}
				var id string
				if err = row.Scan(&id); err != nil {
					return err
				}
				attemptBusIDs = append(attemptBusIDs, id)
			}
		}
		busIDs = attemptBusIDs

		return nil
	}, query.WithIdempotent())
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "text/html")
	writer.WriteHeader(http.StatusOK)

	_, _ = io.WriteString(writer, `Bus table<br />
<br />
<table border="1">
	<tr>
		<th>ID</th>
		<th>Get free seats link</th>
		<th>Buy ticket link</th>
	</tr>
`)
	for _, id := range busIDs {
		_, _ = fmt.Fprintf(writer, `<tr>
	<td>%v</td>
	<td><a href="/get/%v">/get/%v</a></td>
	<td><a href="/buy/%v">/buy/%v</a></td>
</tr>`, id, id, id, id, id)
	}
	_, _ = io.WriteString(writer, "</table>")
}
