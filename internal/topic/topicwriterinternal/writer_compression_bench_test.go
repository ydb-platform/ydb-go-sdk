package topicwriterinternal

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
)

// BenchmarkWriterCompression measures the message compression path without a
// server, background writer goroutines, or ACK latency. Writers share one custom
// encoder option, as applications can do when opening multiple producer sessions.
// Each operation encodes one message using an existing writer.
// Sequential round-robin writes isolate cross-writer reuse from concurrent demand.
func BenchmarkWriterCompression(b *testing.B) {
	for _, writers := range []int{1, 300} {
		for _, size := range []int{4 << 10, 256 << 10} {
			b.Run(fmt.Sprintf("writers=%d/bytes=%d", writers, size), func(b *testing.B) {
				benchmarkWriterCompression(b, writers, size)
			})
		}
	}
}

func benchmarkWriterCompression(b *testing.B, writerCount, size int) {
	// A fixed seed and repeated 1 KiB chunks provide reproducible input with both
	// compressible and incompressible content, rather than an all-zero payload.
	payload := make([]byte, size)
	if _, err := rand.New(rand.NewSource(1)).Read(payload); err != nil {
		b.Fatal(err)
	}
	for offset := 1024; offset < len(payload); offset += 2048 {
		copy(payload[offset:offset+1024], payload[offset-1024:offset])
	}

	cfg := NewWriterReconnectorConfig(WithAddEncoder(rawtopiccommon.CodecGzip,
		func(output io.Writer) (io.WriteCloser, error) {
			return gzip.NewWriterLevel(output, gzip.BestSpeed)
		}))
	writers := make([]*WriterReconnector, writerCount)
	for i := range writers {
		writers[i] = newWriterReconnectorStopped(cfg)
		message := topicwritercommon.NewMessageDataWithContent(PublicMessage{
			Data: bytes.NewReader(payload),
		}, writers[i].encodersMap)
		if err := message.CacheMessageData(rawtopiccommon.CodecGzip); err != nil {
			b.Fatal(err)
		}
	}

	// Keep normal GC behavior: sync.Pool retention is part of the workload.
	// Setup and initial warm-up are excluded from both time and allocation counts.
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	var encoded []byte
	index := 0
	for b.Loop() {
		message := topicwritercommon.NewMessageDataWithContent(PublicMessage{
			Data: bytes.NewReader(payload),
		}, writers[index].encodersMap)
		if err := message.CacheMessageData(rawtopiccommon.CodecGzip); err != nil {
			b.Fatal(err)
		}
		encoded = message.BufEncoded.Bytes()
		index = (index + 1) % len(writers)
	}

	// Check a real compressed result outside the measured section.
	reader, err := gzip.NewReader(bytes.NewReader(encoded))
	if err != nil {
		b.Fatal(err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		b.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		b.Fatal(err)
	}
	if !bytes.Equal(payload, decoded) {
		b.Fatal("decompressed message differs from input")
	}
}
