package topicwriterinternal

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func TestEncoderSelector_CodecMeasure(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		s := NewEncoderSelector(testCommonEncoders, nil)
		_, err := s.measureCodecs(nil)
		require.Error(t, err)
	})
	t.Run("One", func(t *testing.T) {
		s := NewEncoderSelector(NewEncoderMap(), rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw})
		codec, err := s.measureCodecs(nil)
		require.NoError(t, err)
		require.Equal(t, rawtopiccommon.CodecRaw, codec)
	})

	t.Run("SelectCodecByMeasure", func(t *testing.T) {
		// for reproducible result between runs
		rand.Seed(0)
		const (
			smallSize = 1
			largeSize = 100
		)

		testSelectCodec := func(t testing.TB, targetCodec rawtopiccommon.Codec, smallCount, largeCount int) {
			s := NewEncoderSelector(testCommonEncoders, rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw, rawtopiccommon.CodecGzip})

			var messages []messageWithDataContent
			for i := 0; i < smallCount; i++ {
				data := make([]byte, smallSize)
				message, err := newMessageDataWithContent(Message{Data: bytes.NewReader(data)}, testCommonEncoders, codecUnknown)
				require.NoError(t, err)
				messages = append(messages, message)
			}

			for i := 0; i < largeCount; i++ {
				data := make([]byte, largeSize)
				message, err := newMessageDataWithContent(Message{Data: bytes.NewReader(data)}, testCommonEncoders, codecUnknown)
				require.NoError(t, err)
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
			rand.Shuffle(len(messages), func(i, k int) {
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
