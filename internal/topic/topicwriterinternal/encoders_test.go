package topicwriterinternal

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestEncoderSelector_CodecMeasure(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		s := NewEncoderSelector(testCommonEncoders, nil, 1, &trace.Topic{}, "", "")
		_, err := s.measureCodecs(nil)
		require.Error(t, err)
	})
	t.Run("One", func(t *testing.T) {
		s := NewEncoderSelector(
			NewEncoderMap(),
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
			s := NewEncoderSelector(testCommonEncoders, rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecRaw,
				rawtopiccommon.CodecGzip,
			}, 4,
				&trace.Topic{}, "", "",
			)

			var messages []messageWithDataContent
			for i := 0; i < smallCount; i++ {
				data := make([]byte, smallSize)
				message := newMessageDataWithContent(PublicMessage{Data: bytes.NewReader(data)}, testCommonEncoders)
				messages = append(messages, message)
			}

			for i := 0; i < largeCount; i++ {
				data := make([]byte, largeSize)
				message := newMessageDataWithContent(PublicMessage{Data: bytes.NewReader(data)}, testCommonEncoders)
				messages = append(messages, message)
			}

			codec, err := s.measureCodecs(messages)
			require.NoError(t, err)
			require.Equal(t, targetCodec, codec)

			// reverse
			{
				reverseMessages := make([]messageWithDataContent, len(messages))
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
		require.NoError(t, cacheMessages(nil, rawtopiccommon.CodecRaw, 1))
	})

	t.Run("RawOk", func(t *testing.T) {
		messages := newTestMessagesWithContent(1)
		require.NoError(t, cacheMessages(messages, rawtopiccommon.CodecRaw, 1))
	})
	t.Run("RawError", func(t *testing.T) {
		mess := newMessageDataWithContent(PublicMessage{}, testCommonEncoders)
		_, err := mess.GetEncodedBytes(rawtopiccommon.CodecGzip)
		require.NoError(t, err)
		messages := []messageWithDataContent{mess}
		require.Error(t, cacheMessages(messages, rawtopiccommon.CodecRaw, 1))
	})

	const messageCount = 10
	t.Run("GzipOneThread", func(t *testing.T) {
		var messages []messageWithDataContent
		for i := 0; i < messageCount; i++ {
			mess := newMessageDataWithContent(PublicMessage{Data: strings.NewReader("asdf")}, testCommonEncoders)
			messages = append(messages, mess)
		}

		require.NoError(t, cacheMessages(messages, rawtopiccommon.CodecGzip, 1))
		for i := 0; i < messageCount; i++ {
			require.Equal(t, rawtopiccommon.CodecGzip, messages[i].bufCodec)
		}
	})

	const parallelCount = 10
	t.Run("GzipOk", func(t *testing.T) {
		var messages []messageWithDataContent
		for i := 0; i < messageCount; i++ {
			mess := newMessageDataWithContent(PublicMessage{Data: strings.NewReader("asdf")}, testCommonEncoders)
			messages = append(messages, mess)
		}

		require.NoError(t, cacheMessages(messages, rawtopiccommon.CodecGzip, parallelCount))
		for i := 0; i < messageCount; i++ {
			require.Equal(t, rawtopiccommon.CodecGzip, messages[i].bufCodec)
		}
	})

	t.Run("GzipErr", func(t *testing.T) {
		var messages []messageWithDataContent
		for i := 0; i < messageCount; i++ {
			mess := newMessageDataWithContent(PublicMessage{Data: strings.NewReader("asdf")}, testCommonEncoders)
			messages = append(messages, mess)
		}
		messages[0].dataWasRead = true

		require.Error(t, cacheMessages(messages, rawtopiccommon.CodecGzip, parallelCount))
	})
}

func TestEncodersPool(t *testing.T) {
	decompressGzip := func(rd io.Reader) string {
		gzreader, err := gzip.NewReader(rd)
		require.NoError(t, err)

		decompressedMsg, err := io.ReadAll(gzreader)
		require.NoError(t, err)

		return string(decompressedMsg)
	}

	t.Run("NotResetableWriter", func(t *testing.T) {
		testEncoderMap := NewEncoderMap()

		buf := &bytes.Buffer{}
		_, err := testEncoderMap.Encode(rawtopiccommon.CodecRaw, buf, []byte("test_data"))
		require.NoError(t, err)
		require.Len(t, testEncoderMap.p, 0)
		require.Equal(t, "test_data", buf.String())
	})

	t.Run("ResetableWriterCustom", func(t *testing.T) {
		testEncoderMap := NewEncoderMap()

		customCodecCode := rawtopiccommon.Codec(5)
		testEncoderMap.AddEncoder(customCodecCode, func(writer io.Writer) (io.WriteCloser, error) {
			return gzip.NewWriter(writer), nil
		})

		buf := &bytes.Buffer{}
		_, err := testEncoderMap.Encode(customCodecCode, buf, []byte("test_data_1"))
		require.NoError(t, err)
		require.Len(t, testEncoderMap.p, 1)
		require.Equal(t, "test_data_1", decompressGzip(buf))

		buf.Reset()
		_, err = testEncoderMap.Encode(rawtopiccommon.CodecGzip, buf, []byte("test_data_2"))
		require.NoError(t, err)
		require.Len(t, testEncoderMap.p, 2)
		require.Equal(t, "test_data_2", decompressGzip(buf))
	})

	t.Run("ResetableWriter", func(t *testing.T) {
		testEncoderMap := NewEncoderMap()

		buf := &bytes.Buffer{}
		_, err := testEncoderMap.Encode(rawtopiccommon.CodecGzip, buf, []byte("test_data_1"))
		require.NoError(t, err)
		require.Len(t, testEncoderMap.p, 1)
		require.Equal(t, "test_data_1", decompressGzip(buf))

		buf.Reset()
		_, err = testEncoderMap.Encode(rawtopiccommon.CodecGzip, buf, []byte("test_data_2"))
		require.NoError(t, err)
		require.Len(t, testEncoderMap.p, 1)
		require.Equal(t, "test_data_2", decompressGzip(buf))
	})

	t.Run("ResetableWriterManyMessages", func(t *testing.T) {
		testEncoderMap := NewEncoderMap()

		buf := &bytes.Buffer{}
		for i := 0; i < 50; i++ {
			testMsg := []byte(fmt.Sprintf("data_%d", i))

			buf.Reset()
			_, err := testEncoderMap.Encode(rawtopiccommon.CodecGzip, buf, testMsg)
			require.NoError(t, err)

			require.Equal(t, string(testMsg), decompressGzip(buf))
		}

		require.Len(t, testEncoderMap.p, 1)
	})
}
