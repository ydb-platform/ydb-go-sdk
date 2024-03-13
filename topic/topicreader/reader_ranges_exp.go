//go:build ydb_experiment && goexperiment.rangefunc

package topicreader

import (
	"context"
	"iter"
)

// RangeMessages allow range over messages, if reader return error while read message
// then err pass to yield and loop stop.
// After loop will be stopped
func (r *Reader) RangeMessages(ctx context.Context) iter.Seq2[*Message, error] {
	return func(yield func(*Message, error) bool) {
		mess, err := r.ReadMessage(ctx)
		cont := yield(mess, err)
		if !cont || err != nil {
			return
		}
	}
}
