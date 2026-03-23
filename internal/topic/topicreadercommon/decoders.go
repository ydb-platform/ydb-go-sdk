package topicreadercommon

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type PublicResetableReader interface {
	io.Reader
	Reset(io.Reader) error
}

type decoderPool struct {
	pool sync.Pool
}

func (p *decoderPool) Get() PublicResetableReader {
	dec, _ := p.pool.Get().(PublicResetableReader)

	return dec
}

func (p *decoderPool) Put(rd PublicResetableReader) {
	p.pool.Put(rd)
}

func newDecoderPool() *decoderPool {
	return &decoderPool{
		pool: sync.Pool{},
	}
}

type MultiDecoder struct {
	m map[rawtopiccommon.Codec]PublicCreateDecoderFunc

	dp map[rawtopiccommon.Codec]*decoderPool
}

func NewMultiDecoder() *MultiDecoder {
	md := &MultiDecoder{
		m:  make(map[rawtopiccommon.Codec]PublicCreateDecoderFunc),
		dp: make(map[rawtopiccommon.Codec]*decoderPool),
	}

	md.AddDecoder(rawtopiccommon.CodecRaw, func(input io.Reader) (io.Reader, error) {
		return input, nil
	})
	md.AddDecoder(rawtopiccommon.CodecGzip, func(input io.Reader) (io.Reader, error) {
		return gzip.NewReader(input)
	})

	return md
}

func (d *MultiDecoder) AddDecoder(codec rawtopiccommon.Codec, createFunc PublicCreateDecoderFunc) {
	d.m[codec] = createFunc
	d.dp[codec] = newDecoderPool()
}

func (d *MultiDecoder) Decode(codec rawtopiccommon.Codec, input io.Reader) (io.Reader, error) {
	dec, err := d.createDecodeReader(codec, input)
	if err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	_, err = buf.ReadFrom(dec)
	if err != nil {
		return nil, err
	}

	if resetableDec, ok := dec.(PublicResetableReader); ok {
		d.dp[codec].Put(resetableDec)
	}

	return buf, nil
}

func (d *MultiDecoder) createDecodeReader(codec rawtopiccommon.Codec, source io.Reader) (io.Reader, error) {
	if dPool, ok := d.dp[codec]; ok {
		rd := dPool.Get()
		if rd != nil {
			if err := rd.Reset(source); err != nil {
				return nil, err
			}
			return rd, nil
		}
	}

	if decoderCreator, ok := d.m[codec]; ok {
		return decoderCreator(source)
	}

	return nil, xerrors.WithStackTrace(xerrors.Wrap(
		fmt.Errorf("ydb: failed decompress message with codec %v: %w", codec, ErrPublicUnexpectedCodec),
	))
}

type PublicCreateDecoderFunc func(input io.Reader) (io.Reader, error)

// ErrPublicUnexpectedCodec return when try to read message content with unknown codec
var ErrPublicUnexpectedCodec = xerrors.Wrap(errors.New("ydb: unexpected codec"))
