package topicwriterinternal

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

const (
	codecMeasureIntervalBatches = 100
	codecUnknown                = rawtopiccommon.CodecUNSPECIFIED
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

	allowedCodecs     rawtopiccommon.SupportedCodecs
	lastSelectedCodec rawtopiccommon.Codec
	batchCounter      int
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
	s.lastSelectedCodec = codecUnknown
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
	if len(s.allowedCodecs) == 0 {
		return codecUnknown, errNoAllowedCodecs
	}
	if len(s.allowedCodecs) == 1 {
		return s.allowedCodecs[0], nil
	}

	s.batchCounter++
	if s.batchCounter < 0 {
		s.batchCounter = 0
	}

	// Try every codec at start - for fast reader fail if unexpected include codec, incompatible with readers
	if s.batchCounter < len(s.allowedCodecs) {
		return s.allowedCodecs[s.batchCounter], nil
	}

	if s.lastSelectedCodec == codecUnknown || s.batchCounter%codecMeasureIntervalBatches == 0 {
		if codec, err := s.measureCodecs(messages); err == nil {
			s.lastSelectedCodec = codec
		} else {
			return codecUnknown, err
		}
	}

	return s.lastSelectedCodec, nil
}

func (s *EncoderSelector) measureCodecs(messages []messageWithDataContent) (rawtopiccommon.Codec, error) {
	if len(s.allowedCodecs) == 0 {
		return codecUnknown, errNoAllowedCodecs
	}
	sizes := make([]int, len(s.allowedCodecs))

	for codecIndex, codec := range s.allowedCodecs {
		if err := s.compressMessages(messages, codec); err != nil {
			return codecUnknown, err
		}

		size := 0
		for messIndex := range messages {
			content, err := messages[messIndex].GetEncodedBytes(codec)
			if err != nil {
				return codecUnknown, err
			}
			size += len(content)
		}
		sizes[codecIndex] = size
	}

	minSizeIndex := 0
	for i := range sizes {
		if sizes[i] < sizes[minSizeIndex] {
			minSizeIndex = i
		}
	}

	return s.allowedCodecs[minSizeIndex], nil
}
