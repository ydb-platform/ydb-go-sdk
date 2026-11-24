package topicwritercommon

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var testCommonEncoders = NewMultiEncoder()

func newTestMessageWithDataContent(num int) MessageWithDataContent {
	res := NewMessageDataWithContent(PublicMessage{SeqNo: int64(num)}, testCommonEncoders)

	return res
}

func newTestMessagesWithContent(numbers ...int) []MessageWithDataContent {
	messages := make([]MessageWithDataContent, 0, len(numbers))
	for _, num := range numbers {
		messages = append(messages, newTestMessageWithDataContent(num))
	}

	return messages
}

func TestEncoderSelector_CodecMeasure(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		s := NewEncoderSelector(context.TODO(), testCommonEncoders, nil, 1, &trace.Topic{}, "", "")
		_, err := s.measureCodecs(nil)
		require.Error(t, err)
	})
	t.Run("One", func(t *testing.T) {
		s := NewEncoderSelector(
			context.TODO(),
			NewMultiEncoder(),
			rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw},
			1,
			&trace.Topic{},
			"",
			"",
		)
		codec, err := s.measureCodecs(nil)
		require.NoError(t, err)
		require.Equal(t, rawtopiccommon.CodecRaw, codec)
	})

	t.Run("SelectCodecByMeasure", func(t *testing.T) {
		// for reproducible result between runs
		r := xrand.New(xrand.WithSeed(0))
		const (
			smallSize = 1
			largeSize = 100
		)

		testSelectCodec := func(t testing.TB, targetCodec rawtopiccommon.Codec, smallCount, largeCount int) {
			s := NewEncoderSelector(context.TODO(), testCommonEncoders, rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecRaw,
				rawtopiccommon.CodecGzip,
			}, 4,
				&trace.Topic{}, "", "",
			)

			var messages []MessageWithDataContent
			for range smallCount {
				data := make([]byte, smallSize)
				message := NewMessageDataWithContent(PublicMessage{Data: bytes.NewReader(data)}, testCommonEncoders)
				messages = append(messages, message)
			}

			for range largeCount {
				data := make([]byte, largeSize)
				message := NewMessageDataWithContent(PublicMessage{Data: bytes.NewReader(data)}, testCommonEncoders)
				messages = append(messages, message)
			}

			codec, err := s.measureCodecs(messages)
			require.NoError(t, err)
			require.Equal(t, targetCodec, codec)

			// reverse
			{
				reverseMessages := make([]MessageWithDataContent, len(messages))
				for index := range messages {
					reverseMessages[index] = messages[len(messages)-index-1]
				}
				messages = reverseMessages
			}
			codec, err = s.measureCodecs(messages)
			require.NoError(t, err)
			require.Equal(t, targetCodec, codec)

			// shuffle
			r.Shuffle(len(messages), func(i, k int) {
				messages[i], messages[k] = messages[k], messages[i]
			})
			codec, err = s.measureCodecs(messages)
			require.NoError(t, err)
			require.Equal(t, targetCodec, codec)
		}

		table := []struct {
			name        string
			smallCount  int
			largeCount  int
			targetCodec rawtopiccommon.Codec
		}{
			{
				"OneSmall",
				1,
				0,
				rawtopiccommon.CodecRaw,
			},
			{
				"ManySmall",
				10,
				0,
				rawtopiccommon.CodecRaw,
			},
			{
				"OneLarge",
				0,
				1,
				rawtopiccommon.CodecGzip,
			},
			{
				"ManyLarge",
				0,
				1,
				rawtopiccommon.CodecGzip,
			},
			{
				"OneSmallOneLarge",
				1,
				1,
				rawtopiccommon.CodecGzip,
			},
			{
				"ManySmallOneLarge",
				100,
				1,
				rawtopiccommon.CodecRaw,
			},
			{
				"OneSmallManyLarge",
				1,
				10,
				rawtopiccommon.CodecGzip,
			},
			{
				"ManySmallManyLarge",
				10,
				10,
				rawtopiccommon.CodecGzip,
			},
		}

		for _, test := range table {
			t.Run(test.name, func(t *testing.T) {
				testSelectCodec(t, test.targetCodec, test.smallCount, test.largeCount)
			})
		}
	})
}

func TestEncoderSelector_CompressMessages(t *testing.T) {
	t.Run("ReportsSizes", func(t *testing.T) {
		var done trace.TopicWriterCompressMessagesDoneInfo
		tracer := &trace.Topic{
			OnWriterCompressMessages: func(
				trace.TopicWriterCompressMessagesStartInfo,
			) func(trace.TopicWriterCompressMessagesDoneInfo) {
				return func(info trace.TopicWriterCompressMessagesDoneInfo) {
					done = info
				}
			},
		}

		s := NewEncoderSelector(
			t.Context(),
			testCommonEncoders,
			rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw},
			1,
			tracer,
			"",
			"",
		)

		const payload = "hello world"
		messages := []MessageWithDataContent{
			NewMessageDataWithContent(PublicMessage{Data: strings.NewReader(payload)}, testCommonEncoders),
		}

		codec, err := s.CompressMessages(messages)
		require.NoError(t, err)
		require.Equal(t, rawtopiccommon.CodecRaw, codec)

		require.NoError(t, done.Error)
		require.Equal(t, len(payload), done.UncompressedSize)
		require.Equal(t, len(payload), done.CompressedSize)
	})

	t.Run("ReportsErrorWithZeroSizes", func(t *testing.T) {
		var done trace.TopicWriterCompressMessagesDoneInfo
		tracer := &trace.Topic{
			OnWriterCompressMessages: func(
				trace.TopicWriterCompressMessagesStartInfo,
			) func(trace.TopicWriterCompressMessagesDoneInfo) {
				return func(info trace.TopicWriterCompressMessagesDoneInfo) {
					done = info
				}
			},
		}

		s := NewEncoderSelector(
			t.Context(),
			testCommonEncoders,
			rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw},
			1,
			tracer,
			"",
			"",
		)

		// pre-cache with a different codec so CodecRaw caching below fails,
		// the same way TestCompressMessages/RawError does it.
		mess := NewMessageDataWithContent(PublicMessage{}, testCommonEncoders)
		_, err := mess.GetEncodedBytes(rawtopiccommon.CodecGzip)
		require.NoError(t, err)

		_, err = s.CompressMessages([]MessageWithDataContent{mess})
		require.Error(t, err)

		require.Error(t, done.Error)
		require.Zero(t, done.UncompressedSize)
		require.Zero(t, done.CompressedSize)
	})
}

func TestUncompressedContentSize(t *testing.T) {
	messages := []MessageWithDataContent{
		NewMessageDataWithContent(PublicMessage{Data: strings.NewReader("ab")}, testCommonEncoders),
		NewMessageDataWithContent(PublicMessage{Data: strings.NewReader("cde")}, testCommonEncoders),
	}
	require.NoError(t, CacheMessages(messages, rawtopiccommon.CodecRaw, 1))

	require.Equal(t, 5, UncompressedContentSize(messages))
}

func TestCompressedContentSize(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		messages := []MessageWithDataContent{
			NewMessageDataWithContent(PublicMessage{Data: strings.NewReader("ab")}, testCommonEncoders),
			NewMessageDataWithContent(PublicMessage{Data: strings.NewReader("cde")}, testCommonEncoders),
		}
		require.NoError(t, CacheMessages(messages, rawtopiccommon.CodecRaw, 1))

		compressedSize, err := CompressedContentSize(messages, rawtopiccommon.CodecRaw)
		require.NoError(t, err)
		require.Equal(t, 5, compressedSize)
	})

	t.Run("Error", func(t *testing.T) {
		mess := NewMessageDataWithContent(PublicMessage{}, testCommonEncoders)
		_, err := mess.GetEncodedBytes(rawtopiccommon.CodecGzip)
		require.NoError(t, err)

		_, err = CompressedContentSize([]MessageWithDataContent{mess}, rawtopiccommon.CodecRaw)
		require.Error(t, err)
	})
}

func TestCompressMessages(t *testing.T) {
	t.Run("NoMessages", func(t *testing.T) {
		require.NoError(t, CacheMessages(nil, rawtopiccommon.CodecRaw, 1))
	})

	t.Run("RawOk", func(t *testing.T) {
		messages := newTestMessagesWithContent(1)
		require.NoError(t, CacheMessages(messages, rawtopiccommon.CodecRaw, 1))
	})
	t.Run("RawError", func(t *testing.T) {
		mess := NewMessageDataWithContent(PublicMessage{}, testCommonEncoders)
		_, err := mess.GetEncodedBytes(rawtopiccommon.CodecGzip)
		require.NoError(t, err)
		messages := []MessageWithDataContent{mess}
		require.Error(t, CacheMessages(messages, rawtopiccommon.CodecRaw, 1))
	})

	const messageCount = 10
	t.Run("GzipOneThread", func(t *testing.T) {
		messages := make([]MessageWithDataContent, 0, messageCount)
		for range messageCount {
			mess := NewMessageDataWithContent(PublicMessage{Data: strings.NewReader("asdf")}, testCommonEncoders)
			messages = append(messages, mess)
		}

		require.NoError(t, CacheMessages(messages, rawtopiccommon.CodecGzip, 1))
		for i := range messageCount {
			require.Equal(t, rawtopiccommon.CodecGzip, messages[i].BufCodec)
		}
	})

	const parallelCount = 10
	t.Run("GzipOk", func(t *testing.T) {
		messages := make([]MessageWithDataContent, 0, messageCount)
		for range messageCount {
			mess := NewMessageDataWithContent(PublicMessage{Data: strings.NewReader("asdf")}, testCommonEncoders)
			messages = append(messages, mess)
		}

		require.NoError(t, CacheMessages(messages, rawtopiccommon.CodecGzip, parallelCount))
		for i := range messageCount {
			require.Equal(t, rawtopiccommon.CodecGzip, messages[i].BufCodec)
		}
	})

	t.Run("GzipErr", func(t *testing.T) {
		messages := make([]MessageWithDataContent, 0, messageCount)
		for range messageCount {
			mess := NewMessageDataWithContent(PublicMessage{Data: strings.NewReader("asdf")}, testCommonEncoders)
			messages = append(messages, mess)
		}
		messages[0].DataWasRead = true

		require.Error(t, CacheMessages(messages, rawtopiccommon.CodecGzip, parallelCount))
	})
}

func TestMultiEncoder(t *testing.T) {
	decompressGzip := func(rd io.Reader) string {
		gzreader, err := gzip.NewReader(rd)
		require.NoError(t, err)

		decompressedMsg, err := io.ReadAll(gzreader)
		require.NoError(t, err)

		return string(decompressedMsg)
	}

	t.Run("UnsupportedCodec", func(t *testing.T) {
		var output bytes.Buffer
		n, err := NewMultiEncoder().EncodeBytes(rawtopiccommon.Codec(10001), &output, []byte("payload"))
		require.Error(t, err)
		require.Zero(t, n)
		require.Empty(t, output.Bytes())
	})

	t.Run("BuffersPool", func(t *testing.T) {
		testMultiEncoder := NewMultiEncoder()

		buf := &bytes.Buffer{}
		for i := range 50 {
			testMsg := fmt.Appendf(nil, "test_data_%d", i)

			buf.Reset()
			_, err := testMultiEncoder.Encode(rawtopiccommon.CodecGzip, buf, bytes.NewReader(testMsg))
			require.NoError(t, err)

			require.Equal(t, string(testMsg), decompressGzip(buf))
		}
	})

	t.Run("NotResetableWriter", func(t *testing.T) {
		testMultiEncoder := NewMultiEncoder()
		require.Len(t, testMultiEncoder.m, 2)

		buf := &bytes.Buffer{}
		_, err := testMultiEncoder.EncodeBytes(rawtopiccommon.CodecRaw, buf, []byte("test_data"))
		require.NoError(t, err)
		require.Equal(t, "test_data", buf.String())
	})

	t.Run("ResetableWriterCustom", func(t *testing.T) {
		testMultiEncoder := NewMultiEncoder()

		customCodecCode := rawtopiccommon.Codec(10001)
		testMultiEncoder.AddEncoder(customCodecCode, func(writer io.Writer) (io.WriteCloser, error) {
			return gzip.NewWriter(writer), nil
		})
		require.Len(t, testMultiEncoder.m, 3)

		buf := &bytes.Buffer{}
		_, err := testMultiEncoder.EncodeBytes(customCodecCode, buf, []byte("test_data_1"))
		require.NoError(t, err)
		require.Equal(t, "test_data_1", decompressGzip(buf))

		buf.Reset()
		_, err = testMultiEncoder.EncodeBytes(rawtopiccommon.CodecGzip, buf, []byte("test_data_2"))
		require.NoError(t, err)
		require.Equal(t, "test_data_2", decompressGzip(buf))
	})

	t.Run("ResetableWriter", func(t *testing.T) {
		testMultiEncoder := NewMultiEncoder()

		buf := &bytes.Buffer{}
		_, err := testMultiEncoder.EncodeBytes(rawtopiccommon.CodecGzip, buf, []byte("test_data_1"))
		require.NoError(t, err)
		require.Equal(t, "test_data_1", decompressGzip(buf))

		buf.Reset()
		_, err = testMultiEncoder.EncodeBytes(rawtopiccommon.CodecGzip, buf, []byte("test_data_2"))
		require.NoError(t, err)
		require.Equal(t, "test_data_2", decompressGzip(buf))
	})

	t.Run("ResetableWriterManyMessages", func(t *testing.T) {
		testMultiEncoder := NewMultiEncoder()

		buf := &bytes.Buffer{}
		for i := range 50 {
			testMsg := fmt.Appendf(nil, "test_data_%d", i)

			buf.Reset()
			_, err := testMultiEncoder.EncodeBytes(rawtopiccommon.CodecGzip, buf, testMsg)
			require.NoError(t, err)

			require.Equal(t, string(testMsg), decompressGzip(buf))
		}
	})
}

type readerOnly struct {
	r io.Reader
}

func (r readerOnly) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func encodeTestPayload(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = "0123456789abcdef {}\":,\n"[(i*7+i/13)%23]
	}

	return data
}

func TestMultiEncoderEncodeSources(t *testing.T) {
	encoder := NewMultiEncoder()
	for _, size := range []int{0, 1, 511, 512, 513, 32*1024 + 7, 300_000} {
		data := encodeTestPayload(size)
		for _, codec := range []rawtopiccommon.Codec{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip} {
			var want bytes.Buffer
			_, err := encoder.EncodeBytes(codec, &want, data)
			require.NoError(t, err)

			sources := map[string]io.Reader{
				"bytes.Reader":   bytes.NewReader(data),
				"bytes.Buffer":   bytes.NewBuffer(bytes.Clone(data)),
				"strings.Reader": strings.NewReader(string(data)),
				"io.Reader":      readerOnly{bytes.NewReader(data)},
			}
			for name, source := range sources {
				t.Run(fmt.Sprintf("%v/%d/%s", codec, size, name), func(t *testing.T) {
					var got bytes.Buffer
					n, err := encoder.Encode(codec, &got, source)
					require.NoError(t, err)
					require.Equal(t, size, n)
					require.Equal(t, want.Bytes(), got.Bytes())
				})
			}
		}
	}
}

func TestMultiEncoderEncodeUnsupportedCodec(t *testing.T) {
	for name, source := range map[string]io.Reader{
		"WriterTo": bytes.NewReader([]byte("payload")),
		"Reader":   readerOnly{bytes.NewReader([]byte("payload"))},
	} {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			n, err := NewMultiEncoder().Encode(rawtopiccommon.Codec(10001), &output, source)
			require.Error(t, err)
			require.Zero(t, n)
			require.Empty(t, output.Bytes())
		})
	}
}

type writerToOnly struct {
	data []byte
}

func (w writerToOnly) Read([]byte) (int, error) {
	return 0, errors.New("the source must be written with WriteTo, not copied with Read")
}

func (w writerToOnly) WriteTo(target io.Writer) (int64, error) {
	n, err := target.Write(w.data)

	return int64(n), err
}

func TestMultiEncoderEncodeWriterToWithoutCopy(t *testing.T) {
	encoder := NewMultiEncoder()
	data := encodeTestPayload(1000)
	for _, codec := range []rawtopiccommon.Codec{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip} {
		var want, got bytes.Buffer
		_, err := encoder.EncodeBytes(codec, &want, data)
		require.NoError(t, err)

		n, err := encoder.Encode(codec, &got, writerToOnly{data: data})
		require.NoError(t, err)
		require.Equal(t, len(data), n)
		require.Equal(t, want.Bytes(), got.Bytes())
	}
}

type recordingEncoder struct {
	calls *[]string
}

func (r recordingEncoder) Write(p []byte) (int, error) {
	*r.calls = append(*r.calls, "write")

	return len(p), nil
}

func (r recordingEncoder) Close() error {
	*r.calls = append(*r.calls, "close")

	return nil
}

func newRecordingMultiEncoder(calls *[]string) *MultiEncoder {
	encoder := NewMultiEncoder()
	encoder.AddEncoder(rawtopiccommon.CodecLzop, func(io.Writer) (io.WriteCloser, error) {
		*calls = append(*calls, "create")

		return recordingEncoder{calls: calls}, nil
	})

	return encoder
}

type failingWriterTo struct{}

func (failingWriterTo) Read([]byte) (int, error) {
	return 0, errors.New("read")
}

func (failingWriterTo) WriteTo(io.Writer) (int64, error) {
	return 0, errors.New("source failed")
}

func TestMultiEncoderEncodeClosesEncoderOnSourceError(t *testing.T) {
	var calls []string
	_, err := newRecordingMultiEncoder(&calls).Encode(rawtopiccommon.CodecLzop, &bytes.Buffer{}, failingWriterTo{})
	require.EqualError(t, err, "source failed")
	created, closed := 0, 0
	for _, call := range calls {
		switch call {
		case "create":
			created++
		case "close":
			closed++
		}
	}
	require.Equal(t, created, closed, "every created encoder is closed: %v", calls)
}

func TestMultiEncoderEncodeEmptyDataWritesBeforeClose(t *testing.T) {
	for name, source := range map[string]io.Reader{
		"bytes.Reader":   &bytes.Reader{},
		"bytes.Buffer":   &bytes.Buffer{},
		"strings.Reader": strings.NewReader(""),
		"io.Reader":      readerOnly{&bytes.Reader{}},
	} {
		t.Run(name, func(t *testing.T) {
			var calls []string
			n, err := newRecordingMultiEncoder(&calls).Encode(rawtopiccommon.CodecLzop, &bytes.Buffer{}, source)
			require.NoError(t, err)
			require.Zero(t, n)
			require.Equal(t, []string{"create", "write", "close"}, calls)
		})
	}
}

func BenchmarkMultiEncoderEncode(b *testing.B) {
	for _, size := range []int{4 << 10, 256 << 10} {
		data := encodeTestPayload(size)
		str := string(data)
		sources := []struct {
			name  string
			reset func() io.Reader
		}{
			{"bytes.Reader", func() io.Reader { return bytes.NewReader(data) }},
			{"strings.Reader", func() io.Reader { return strings.NewReader(str) }},
		}
		for _, codec := range []rawtopiccommon.Codec{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip} {
			for _, source := range sources {
				for _, pool := range []string{"warm", "cold"} {
					name := fmt.Sprintf("codec=%v/size=%dKiB/source=%s/pool=%s", codec, size>>10, source.name, pool)
					b.Run(name, func(b *testing.B) {
						encoder := NewMultiEncoder()
						var target bytes.Buffer
						b.SetBytes(int64(size))
						b.ReportAllocs()
						for b.Loop() {
							if pool == "cold" {
								encoder.bp = xsync.Pool[bytes.Buffer]{New: func() *bytes.Buffer { return &bytes.Buffer{} }}
							}
							target.Reset()
							if _, err := encoder.Encode(codec, &target, source.reset()); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}
