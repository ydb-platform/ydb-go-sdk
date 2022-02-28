package main

import (
	"log"
	"os"
)

//go:generate gtrace -tag gtrace

//gtrace:gen
//gtrace:set shortcut
type Trace struct {
	OnInput func(string) func()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("[logs] ")

	t := logTrace(Trace{})
	done := traceOnInput(t, os.Args[1])
	defer done()
}

func logTrace(t Trace) Trace {
	return t.Compose(Trace{
		OnInput: func(s string) func() {
			log.Printf("processing %q", s)
			return func() {
				log.Printf("processed %q", s)
			}
		},
	})
}
