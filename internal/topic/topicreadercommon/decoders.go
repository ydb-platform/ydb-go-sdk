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

type ReadResetter interface {
	io.Reader
	Reset(r io.Reader) error
}

type decoderPool struct {
	pool sync.Pool
}

func (p *decoderPool) Get() ReadResetter {
	dec, _ := p.pool.Get().(ReadResetter)

	return dec
}

func (p *decoderPool) Put(dec ReadResetter) {
	p.pool.Put(dec)
}

func newDecoderPool() *decoderPool {
	return &decoderPool{
		pool: sync.Pool{},
	}
}

type DecoderMap struct {
	m  map[rawtopiccommon.Codec]func(io.Reader) (ReadResetter, error)
	dp map[rawtopiccommon.Codec]*decoderPool
}

func NewDecoderMap() DecoderMap {
	dm := DecoderMap{
		m:  make(map[rawtopiccommon.Codec]func(io.Reader) (ReadResetter, error)),
		dp: make(map[rawtopiccommon.Codec]*decoderPool),
	}

	dm.AddDecoder(rawtopiccommon.CodecRaw, func(input io.Reader) (ReadResetter, error) {
		return &nopResetter{Reader: input}, nil
	})

	dm.AddDecoder(rawtopiccommon.CodecGzip, func(input io.Reader) (ReadResetter, error) {
		gz, err := gzip.NewReader(input)
		if err != nil {
			return nil, err
		}
		return gz, nil
	})

	return dm
}

func (m *DecoderMap) AddDecoder(codec rawtopiccommon.Codec, createFunc func(io.Reader) (ReadResetter, error)) {
	m.m[codec] = createFunc
	m.dp[codec] = newDecoderPool()
}

type pooledDecoder struct {
	ReadResetter
	pool *decoderPool
}

func (p *pooledDecoder) Close() error {
	if closer, ok := p.ReadResetter.(io.Closer); ok {
		closer.Close()
	}
	p.pool.Put(p.ReadResetter)

	return nil
}

func (m *DecoderMap) Decode(codec rawtopiccommon.Codec, input io.Reader) (io.Reader, error) {
	createFunc, ok := m.m[codec]
	if !ok {
		return nil, xerrors.WithStackTrace(xerrors.Wrap(
			fmt.Errorf("ydb: failed decompress message with codec %v: %w", codec, ErrPublicUnexpectedCodec),
		))
	}

	pool := m.dp[codec]
	decoder := pool.Get()
	if decoder == nil {
		var err error
		decoder, err = createFunc(input)
		if err != nil {
			return nil, err
		}
	} else {
		if err := decoder.Reset(input); err != nil {
			return nil, err
		}
	}

	return &pooledDecoder{
		ReadResetter: decoder,
		pool:         pool,
	}, nil
}

type nopResetter struct {
	io.Reader
}

func (n *nopResetter) Reset(r io.Reader) error {
	n.Reader = r

	return nil
}

type PublicCreateDecoderFunc func(input io.Reader) (ReadResetter, error)

// ErrPublicUnexpectedCodec return when try to read message content with unknown codec
var ErrPublicUnexpectedCodec = xerrors.Wrap(errors.New("ydb: unexpected codec"))
