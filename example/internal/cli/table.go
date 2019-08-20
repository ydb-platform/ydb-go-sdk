package cli

import (
	"flag"
	"log"

	"github.com/yandex-cloud/ydb-go-sdk/internal/traceutil"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

func ExportTableClient(flag *flag.FlagSet) func() *table.Client {
	var (
		client table.Client
		trace  bool
	)
	flag.BoolVar(&trace,
		"client-trace", false,
		"trace all client events",
	)
	return func() *table.Client {
		if trace {
			var ctrace table.ClientTrace
			traceutil.Stub(&ctrace, func(name string, args ...interface{}) {
				log.Printf(
					"[client] %s: %+v",
					name, traceutil.ClearContext(args),
				)
			})
			client.Trace = ctrace
		}
		return &client
	}
}

func ExportSessionPool(flag *flag.FlagSet) func() *table.SessionPool {
	var (
		pool  table.SessionPool
		trace bool
	)
	flag.BoolVar(&trace,
		"session-pool-trace", false,
		"trace all session pool events",
	)
	return func() *table.SessionPool {
		if trace {
			var strace table.SessionPoolTrace
			traceutil.Stub(&strace, func(name string, args ...interface{}) {
				log.Printf(
					"[session pool] %s: %+v",
					name, traceutil.ClearContext(args),
				)
			})
			pool.Trace = strace
		}
		return &pool
	}
}
