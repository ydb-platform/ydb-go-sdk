package topicwriterinternal

import (
	"bytes"
	"compress/gzip"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Codec int

const (
	CodecAuto Codec = 0
	CodecRaw        = Codec(rawtopiccommon.CodecRaw)
	CodecGZip       = Codec(rawtopiccommon.CodecGzip)
)

func selectCodec(messages []messageWithDataContent, allowCodecs []Codec) rawtopiccommon.Codec {
	for i := range messages {
		if messages[i].bufCodec != rawtopiccommon.CodecRaw {
			panic("ydb: non raw codec for select codec message, must be never")
		}
		if messages[i].buf.Len() > gzipHeaderSize {
			return rawtopiccommon.CodecGzip
		}
	}

	return rawtopiccommon.CodecRaw
}

var gzipHeaderSize int

func init() {
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	_, _ = w.Write(nil)
	_ = w.Close()
	gzipHeaderSize = buf.Len()
}

func compressMessage(m *messageWithDataContent, c rawtopiccommon.Codec) error {
	if m.bufCodec != rawtopiccommon.CodecRaw {
		return xerrors.NewYdbErrWithStackTrace("ydb: bad internal state, try to re-compress message")
	}

	if c != rawtopiccommon.CodecRaw {
		panic("not implemented")
	}

	return nil
}
