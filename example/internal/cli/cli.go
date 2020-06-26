package cli

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

var ErrPrintUsage = fmt.Errorf("")

type Parameters struct {
	Endpoint string
	Database string
	Path     string

	Args []string
}

type Command interface {
	Run(context.Context, Parameters) error
	ExportFlags(context.Context, *flag.FlagSet)
}

type CommandFunc func(context.Context, Parameters) error

func (f CommandFunc) Run(ctx context.Context, params Parameters) error {
	return f(ctx, params)
}

func (f CommandFunc) ExportFlags(context.Context, *flag.FlagSet) {}

func Run(cmd Command) {
	flag := flag.NewFlagSet("example", flag.ExitOnError)

	var params Parameters
	flag.StringVar(&params.Endpoint,
		"endpoint", "",
		"endpoint url to use",
	)
	flag.StringVar(&params.Path,
		"path", "",
		"tables path",
	)
	flag.StringVar(&params.Database,
		"database", "",
		"name of the database to use",
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.ExportFlags(ctx, flag)

	_ = flag.Parse(os.Args[1:])
	params.Args = flag.Args()

	quit := make(chan error)
	go processSignals(map[os.Signal]func(){
		syscall.SIGINT: func() {
			if ctx.Err() != nil {
				quit <- fmt.Errorf("forced quit")
			}
			cancel()
		},
	})

	log.SetFlags(0)

	done := make(chan error)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				buf := make([]byte, 64<<10)
				buf = buf[:runtime.Stack(buf, false)]
				done <- fmt.Errorf("panic recovered: %v\n%s", e, buf)
			}
		}()
		done <- cmd.Run(ctx, params)
	}()

	var err error
	select {
	case err = <-done:
	case err = <-quit:
	}
	if err == ErrPrintUsage {
		flag.Usage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func processSignals(m map[os.Signal]func()) {
	ch := make(chan os.Signal, len(m))
	for sig := range m {
		signal.Notify(ch, sig)
	}
	for sig := range ch {
		log.Printf("signal received: %s", sig)
		m[sig]()
	}
}
