package topicreaderinternal

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type decoderMap struct {
	m map[rawtopiccommon.Codec]PublicCreateDecoderFunc
}

func newDecoderMap() decoderMap {
	return decoderMap{
		m: map[rawtopiccommon.Codec]PublicCreateDecoderFunc{
			rawtopiccommon.CodecRaw: func(input io.Reader) (io.Reader, error) {
				return input, nil
			},
			rawtopiccommon.CodecGzip: func(input io.Reader) (io.Reader, error) {
				return gzip.NewReader(input)
			},
		},
	}
}

func (m *decoderMap) AddDecoder(codec rawtopiccommon.Codec, createFunc PublicCreateDecoderFunc) {
	m.m[codec] = createFunc
}

func (m *decoderMap) Decode(codec rawtopiccommon.Codec, input io.Reader) (io.Reader, error) {
	if f := m.m[codec]; f != nil {
		return f(input)
	}

	return nil, xerrors.WithStackTrace(xerrors.Wrap(
		fmt.Errorf("ydb: failed decompress message with codec %v: %w", codec, ErrPublicUnexpectedCodec),
	))
}

type PublicCreateDecoderFunc func(input io.Reader) (io.Reader, error)
