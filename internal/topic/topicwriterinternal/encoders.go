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

func NewEncoderMap() *EncoderMap {
	return &EncoderMap{
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

// EncoderSelector not thread safe
type EncoderSelector struct {
	m *EncoderMap

	allowedCodecs rawtopiccommon.SupportedCodecs
	forceCodec    rawtopiccommon.Codec
	batchCounter  int
}

func NewEncoderSelector(m *EncoderMap, allowedCodecs rawtopiccommon.SupportedCodecs) EncoderSelector {
	res := EncoderSelector{
		m: m,
	}
	res.ResetAllowedCodecs(allowedCodecs)

	return res
}

func (s *EncoderSelector) CompressMessages(messages []messageWithDataContent) (rawtopiccommon.Codec, error) {
	codec, err := s.selectCodec(messages)
	if err == nil {
		err = s.compressMessages(messages, codec)
	}
	return codec, err
}

func (s *EncoderSelector) ResetAllowedCodecs(allowedCodecs rawtopiccommon.SupportedCodecs) {
	if s.allowedCodecs.IsEqualsTo(allowedCodecs) {
		return
	}

	s.allowedCodecs = allowedCodecs.Clone()
	if len(s.allowedCodecs) == 1 {
		s.forceCodec = s.allowedCodecs[0]
	} else {
		s.forceCodec = rawtopiccommon.CodecUNSPECIFIED
	}
	s.batchCounter = 0
}

func (s *EncoderSelector) compressMessages(messages []messageWithDataContent, codec rawtopiccommon.Codec) error {
	for i := range messages {
		// force call GetEncodedBytes for cache result
		// it call here for future do it in parallel for
		_, err := messages[i].GetEncodedBytes(codec)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *EncoderSelector) selectCodec(messages []messageWithDataContent) (rawtopiccommon.Codec, error) {
	if s.forceCodec != rawtopiccommon.CodecUNSPECIFIED {
		return s.forceCodec, nil
	}

	if len(s.allowedCodecs) == 0 {
		return rawtopiccommon.CodecUNSPECIFIED, errNoAllowedCodecs
	}

	s.batchCounter++
	if s.batchCounter < 0 {
		s.batchCounter = 0
	}

	index := s.batchCounter % len(s.allowedCodecs)

	return s.allowedCodecs[index], nil
}
