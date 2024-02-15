package topicwriterinternal

import (
	"compress/gzip"
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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

	tracer              *trace.Topic
	writerReconnectorID string
	sessionID           string

	allowedCodecs          rawtopiccommon.SupportedCodecs
	lastSelectedCodec      rawtopiccommon.Codec
	parallelCompressors    int
	batchCounter           int
	measureIntervalBatches int
}

func NewEncoderSelector(
	m *EncoderMap,
	allowedCodecs rawtopiccommon.SupportedCodecs,
	parallelCompressors int,
	tracer *trace.Topic,
	writerReconnectorID, sessionID string,
) EncoderSelector {
	if parallelCompressors <= 0 {
		panic("ydb: need leas one allowed compressor")
	}

	res := EncoderSelector{
		m:                      m,
		parallelCompressors:    parallelCompressors,
		measureIntervalBatches: codecMeasureIntervalBatches,
		tracer:                 tracer,
		writerReconnectorID:    writerReconnectorID,
		sessionID:              sessionID,
	}
	res.ResetAllowedCodecs(allowedCodecs)

	return res
}

func (s *EncoderSelector) CompressMessages(messages []messageWithDataContent) (rawtopiccommon.Codec, error) {
	codec, err := s.selectCodec(messages)
	if err == nil {
		onCompressDone := trace.TopicOnWriterCompressMessages(
			s.tracer,
			s.writerReconnectorID,
			s.sessionID,
			codec.ToInt32(),
			messages[0].SeqNo,
			len(messages),
			trace.TopicWriterCompressMessagesReasonCompressData,
		)
		err = cacheMessages(messages, codec, s.parallelCompressors)
		onCompressDone(err)
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

func (s *EncoderSelector) selectCodec(messages []messageWithDataContent) (rawtopiccommon.Codec, error) {
	if len(s.allowedCodecs) == 0 {
		return codecUnknown, errNoAllowedCodecs
	}
	if len(s.allowedCodecs) == 1 {
		return s.allowedCodecs[0], nil
	}

	defer func() {
		s.batchCounter++
	}()

	if s.batchCounter < 0 {
		s.batchCounter = 0
	}

	// Try every codec at start - for fast reader fail if unexpected include codec, incompatible with readers
	if s.batchCounter < len(s.allowedCodecs) {
		return s.allowedCodecs[s.batchCounter], nil
	}

	if s.lastSelectedCodec == codecUnknown || s.batchCounter%s.measureIntervalBatches == 0 {
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
		firstSeqNo := int64(-1)
		if len(messages) > 0 {
			firstSeqNo = messages[0].SeqNo
		}
		onCompressDone := trace.TopicOnWriterCompressMessages(
			s.tracer,
			s.writerReconnectorID,
			s.sessionID,
			codec.ToInt32(),
			firstSeqNo,
			len(messages),
			trace.TopicWriterCompressMessagesReasonCodecsMeasure,
		)
		err := cacheMessages(messages, codec, s.parallelCompressors)
		onCompressDone(err)
		if err != nil {
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

func cacheMessages(messages []messageWithDataContent, codec rawtopiccommon.Codec, workerCount int) error {
	if len(messages) < workerCount {
		workerCount = len(messages)
	}

	// no need goroutines and synchronization for zero or one worker
	if workerCount < 2 { //nolint:gomnd
		for i := range messages {
			if _, err := messages[i].GetEncodedBytes(codec); err != nil {
				return err
			}
		}
	}

	tasks := make(chan *messageWithDataContent, len(messages))

	for i := range messages {
		tasks <- &messages[i]
	}
	close(tasks)

	var resErrMutex xsync.Mutex
	var resErr error

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()

		for task := range tasks {
			var localErr error
			resErrMutex.WithLock(func() {
				localErr = resErr
			})

			if localErr != nil {
				return
			}
			localErr = task.CacheMessageData(codec)
			if localErr != nil {
				resErrMutex.WithLock(func() {
					resErr = localErr
				})

				return
			}
		}
	}

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}

	wg.Wait()

	return resErr
}
