package main

//go:generate gtrace

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type (
	//gtrace:gen
	//gtrace:set shortcut
	//gtrace:set context
	PingTrace struct {
		OnRequest func(PingTraceRequestStart) func(PingTraceRequestDone)
	}
	PingTraceRequestStart struct {
		Request *http.Request
	}
	PingTraceRequestDone struct {
		Response *http.Response
		Error    error
	}
)

type Pinger struct {
	Trace PingTrace

	wg sync.WaitGroup
}

func (x *Pinger) Ping(ctx context.Context, s string) error {
	u, err := url.ParseRequestURI(s)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("HEAD", u.String(), nil)
	if err != nil {
		return err
	}

	x.wg.Add(1)
	go func() {
		defer x.wg.Done()

		done := pingTraceOnRequest(ctx, x.Trace, req)
		resp, err := http.DefaultClient.Do(req)
		done(resp, err)
	}()

	return nil
}

func (x *Pinger) Close() {
	x.wg.Wait()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("[logs] ")

	var x Pinger
	x.Trace = logTrace(x.Trace)
	defer x.Close()

	s := bufio.NewScanner(os.Stdin)

	var (
		prev     string
		sampling int
	)
	flag.StringVar(&prev,
		"u", "http://ya.ru",
		"default url to ping",
	)
	flag.IntVar(&sampling,
		"sampling", 5,
		"sampling of stats probes",
	)
	flag.Parse()

	fmt.Println("Welcome to pinger!")
	fmt.Println("Default url is set to:", prev)
	fmt.Println("Sampling for latency calculation is set to:", sampling)
	fmt.Println("Type new url or press enter...")

	for i := 0; s.Scan(); i++ {
		ctx := context.Background()
		if i%sampling == 0 {
			ctx = WithPingTrace(ctx, statsTrace(PingTrace{}))
		}
		text := strings.TrimSpace(s.Text())
		if text == "" {
			text = prev
		} else {
			prev = text
		}
		err := x.Ping(ctx, text)
		if err != nil {
			log.Println(err)
		}
	}
}

func logTrace(s PingTrace) PingTrace {
	return s.Compose(PingTrace{
		OnRequest: func(req PingTraceRequestStart) func(PingTraceRequestDone) {
			log.Printf("%s %s...", req.Request.Method, req.Request.URL)
			return func(res PingTraceRequestDone) {
				var (
					status string
					size   int64
				)
				if resp := res.Response; resp != nil {
					status = resp.Status
					size = resp.ContentLength
				}
				log.Printf(
					"%s %s... done; status=%q size=%d err=%v",
					req.Request.Method, req.Request.URL,
					status, size, res.Error,
				)
			}
		},
	})
}

func statsTrace(s PingTrace) PingTrace {
	return s.Compose(PingTrace{
		OnRequest: func(req PingTraceRequestStart) func(PingTraceRequestDone) {
			start := time.Now()
			return func(res PingTraceRequestDone) {
				log.Printf(
					"%s %s    latency: %.2fms",
					req.Request.Method, req.Request.URL,
					time.Since(start).Seconds()*1000,
				)
			}
		},
	})
}
