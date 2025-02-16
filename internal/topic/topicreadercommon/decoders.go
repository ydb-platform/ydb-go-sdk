package topicreadercommon

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type decoderPool struct {
	pool sync.Pool
}

func (p *decoderPool) Get() io.Reader {
	dec, _ := p.pool.Get().(io.Reader)
	
	return dec
}

func (p *decoderPool) Put(dec io.Reader) {
	p.pool.Put(dec)
}

func newDecoderPool() *decoderPool {

	return &decoderPool{
		pool: sync.Pool{},
	}
}

type DecoderMap struct {
	m  map[rawtopiccommon.Codec]PublicCreateDecoderFunc
	dp map[rawtopiccommon.Codec]*decoderPool
}

func NewDecoderMap() DecoderMap {
	dm := DecoderMap{
		m:  make(map[rawtopiccommon.Codec]PublicCreateDecoderFunc),
		dp: make(map[rawtopiccommon.Codec]*decoderPool),
	}

	dm.AddDecoder(rawtopiccommon.CodecRaw, func(input io.Reader) (io.Reader, error) {
		return input, nil
	})

	dm.AddDecoder(rawtopiccommon.CodecGzip, func(input io.Reader) (io.Reader, error) {
		return gzip.NewReader(input)
	})

	return dm
}

func (m *DecoderMap) AddDecoder(codec rawtopiccommon.Codec, createFunc PublicCreateDecoderFunc) {
	m.m[codec] = createFunc
	m.dp[codec] = newDecoderPool()
}

func (m *DecoderMap) Decode(codec rawtopiccommon.Codec, input io.Reader) (io.Reader, error) {
	if f := m.m[codec]; f != nil {
		decoder := m.dp[codec].Get()
		if decoder == nil {
			var err error
			decoder, err = f(input)
			if err != nil {
				return nil, err
			}
		}
		return decoder, nil
	}

	return nil, xerrors.WithStackTrace(xerrors.Wrap(
		fmt.Errorf("ydb: failed decompress message with codec %v: %w", codec, ErrPublicUnexpectedCodec),
	))
}

type PublicCreateDecoderFunc func(input io.Reader) (io.Reader, error)

// ErrPublicUnexpectedCodec return when try to read message content with unknown codec
var ErrPublicUnexpectedCodec = xerrors.Wrap(errors.New("ydb: unexpected codec"))
