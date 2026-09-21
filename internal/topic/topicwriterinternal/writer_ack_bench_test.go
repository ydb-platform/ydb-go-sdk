package topicwriterinternal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// cpu: Apple M3 Pro; Go 1.24.0; medians of 6 runs
// go test -run='^$' -bench=BenchmarkWriterConcurrentSyncWriteSingleACK -benchtime=3s -cpu=4 .
//
// BenchmarkWriterConcurrentSyncWriteSingleACK/inflight=1-4       3480.5 ns/op    2120 B/op    34 allocs/op
// BenchmarkWriterConcurrentSyncWriteSingleACK/inflight=250-4    78407.5 ns/op    2133 B/op    34 allocs/op
// BenchmarkWriterConcurrentSyncWriteSingleACK/inflight=900-4   293480.5 ns/op    2138 B/op    34 allocs/op
func BenchmarkWriterConcurrentSyncWriteSingleACK(b *testing.B) {
	for _, concurrency := range []int{1, 250, 900} {
		b.Run(fmt.Sprintf("inflight=%d", concurrency), func(b *testing.B) {
			benchmarkWriterConcurrentSyncWriteSingleACK(b, concurrency)
		})
	}
}

func benchmarkWriterConcurrentSyncWriteSingleACK(b *testing.B, concurrency int) {
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	stream := &benchmarkACKStream{
		sentSeqNos: make(chan int64, concurrency),
		responses:  make(chan rawtopicwriter.ServerMessage, 1),
		closed:     make(chan struct{}),
	}
	writer, err := NewWriterReconnector(NewWriterReconnectorConfig(
		WithTopic("benchmark-topic"),
		WithProducerID("benchmark-producer"),
		WithCodec(rawtopiccommon.CodecRaw),
		WithWaitAckOnWrite(true),
		WithMaxQueueLen(concurrency),
		WithConnectFunc(func(context.Context, *trace.Topic) (RawTopicWriterStream, error) {
			return stream, nil
		}),
	))
	require.NoError(b, err)

	var workers sync.WaitGroup
	b.Cleanup(func() {
		cancel()
		_ = stream.CloseSend()
		_ = writer.close(context.Background(), context.Canceled)
		workers.Wait()
	})
	require.NoError(b, writer.WaitInit(ctx))

	writeErrors := make(chan error, concurrency)
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			payload := make([]byte, 128)
			reader := bytes.NewReader(payload)
			messages := []PublicMessage{{Data: reader}}
			for ctx.Err() == nil {
				reader.Reset(payload)
				if err := writer.Write(ctx, messages); err != nil {
					writeErrors <- err

					return
				}
			}
		}()
	}

	receiveSentSeqNo := func(want int64) int64 {
		select {
		case seqNo := <-stream.sentSeqNos:
			if seqNo != want {
				b.Fatalf("unexpected sequence number: got %d, want %d", seqNo, want)
			}

			return seqNo
		case err := <-writeErrors:
			b.Fatalf("Write failed: %v", err)

			return 0
		}
	}

	pendingSeqNos := make([]int64, concurrency)
	for i := range pendingSeqNos {
		pendingSeqNos[i] = receiveSentSeqNo(int64(i + 1))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		oldest := i % concurrency
		result := &rawtopicwriter.WriteResult{
			Acks: []rawtopicwriter.WriteAck{{
				SeqNo: pendingSeqNos[oldest],
				MessageWriteStatus: rawtopicwriter.MessageWriteStatus{
					Type:          rawtopicwriter.WriteStatusTypeWritten,
					WrittenOffset: pendingSeqNos[oldest] - 1,
				},
			}},
		}
		result.SetStatus(rawydb.StatusSuccess)
		stream.responses <- result
		pendingSeqNos[oldest] = receiveSentSeqNo(int64(concurrency + i + 1))
	}
	b.StopTimer()
}

type benchmarkACKStream struct {
	sentSeqNos chan int64
	responses  chan rawtopicwriter.ServerMessage
	closed     chan struct{}
	closeOnce  sync.Once
}

func (s *benchmarkACKStream) Send(message rawtopicwriter.ClientMessage) error {
	switch m := message.(type) {
	case *rawtopicwriter.InitRequest:
		result := &rawtopicwriter.InitResult{
			SessionID:       "benchmark-session",
			SupportedCodecs: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw},
		}
		result.SetStatus(rawydb.StatusSuccess)
		select {
		case s.responses <- result:
			return nil
		case <-s.closed:
			return io.EOF
		}
	case *rawtopicwriter.WriteRequest:
		for i := range m.Messages {
			select {
			case s.sentSeqNos <- m.Messages[i].SeqNo:
			case <-s.closed:
				return io.EOF
			}
		}

		return nil
	default:
		return fmt.Errorf("unexpected benchmark stream request: %T", message)
	}
}

func (s *benchmarkACKStream) Recv() (rawtopicwriter.ServerMessage, error) {
	select {
	case message := <-s.responses:
		return message, nil
	case <-s.closed:
		return nil, io.EOF
	}
}

func (s *benchmarkACKStream) CloseSend() error {
	s.closeOnce.Do(func() { close(s.closed) })

	return nil
}

func (*benchmarkACKStream) Endpoint() endpoint.Endpoint {
	return nil
}
