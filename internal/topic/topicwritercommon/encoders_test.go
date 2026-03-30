package topicwritercommon

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
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
		require.Len(t, testMultiEncoder.ep, 2)

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
		require.Len(t, testMultiEncoder.ep, 3)

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
