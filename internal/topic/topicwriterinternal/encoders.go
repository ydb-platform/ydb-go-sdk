package topicwriterinternal

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type EncoderMap struct {
	m map[rawtopiccommon.Codec]PublicCreateEncoderFunc
}

func NewEncoderMap() EncoderMap {
	return EncoderMap{
		m: map[rawtopiccommon.Codec]PublicCreateEncoderFunc{
			rawtopiccommon.CodecRaw: func(writer io.Writer) (io.WriteCloser, error) {
				return nopWriteCloser{writer}, nil
			},
			rawtopiccommon.CodecGzip: func(writer io.Writer) (io.WriteCloser, error) {
				return gzip.NewWriter(writer), nil
			},
		},
	}
}

func (e *EncoderMap) AddEncoder(codec rawtopiccommon.Codec, creator PublicCreateEncoderFunc) {
	e.m[codec] = creator
}

func (e *EncoderMap) CreateLazyEncodeWriter(codec rawtopiccommon.Codec, target io.Writer) (io.WriteCloser, error) {
	if encoderCreator, ok := e.m[codec]; ok {
		return encoderCreator(target)
	}
	return nil, xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: unexpected codec '%v' for encode message", codec)))
}

func (e *EncoderMap) GetSupportedCodecs() rawtopiccommon.SupportedCodecs {
	res := make(rawtopiccommon.SupportedCodecs, 0, len(e.m))
	for codec := range e.m {
		res = append(res, codec)
	}
	return res
}

func (e *EncoderMap) IsSupported(codec rawtopiccommon.Codec) bool {
	_, ok := e.m[codec]
	return ok
}

type PublicCreateEncoderFunc func(writer io.Writer) (io.WriteCloser, error)

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error {
	return nil
}
