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

type PublicResettableReader interface {
	io.Reader
	Reset(rd io.Reader) error
}

type resettableDecoderPool struct {
	pool sync.Pool
}

func (p *resettableDecoderPool) Get() PublicResettableReader {
	dec, _ := p.pool.Get().(PublicResettableReader)

	return dec
}

func (p *resettableDecoderPool) Put(rd PublicResettableReader) {
	p.pool.Put(rd)
}

func newResettableDecoderPool() *resettableDecoderPool {
	return &resettableDecoderPool{}
}

type resettableDecoderWrapper struct {
	rd     PublicResettableReader
	pool   *resettableDecoderPool
	closed bool
}

func wrapResettableDecoder(rd PublicResettableReader, pool *resettableDecoderPool) *resettableDecoderWrapper {
	return &resettableDecoderWrapper{
		rd:     rd,
		pool:   pool,
		closed: false,
	}
}

func (w *resettableDecoderWrapper) Read(p []byte) (n int, err error) {
	if w.closed {
		return 0, io.EOF
	}

	n, err = w.rd.Read(p)
	if errors.Is(err, io.EOF) {
		w.Close()
	}

	return n, err
}

func (w *resettableDecoderWrapper) Close() error {
	if !w.closed {
		w.closed = true
		w.pool.Put(w.rd)
		w.rd = nil
		w.pool = nil
	}

	return nil
}

type decoderCreator struct {
	create PublicCreateDecoderFunc
	pool   *resettableDecoderPool
}

func (c *decoderCreator) getDecoder(input io.Reader) (io.Reader, error) {
	if c.pool == nil {
		return c.create(input)
	}

	if rd := c.pool.Get(); rd != nil {
		if err := rd.Reset(input); err != nil {
			return nil, err
		}

		return wrapResettableDecoder(rd, c.pool), nil
	}

	dec, err := c.create(input)
	if err != nil {
		return nil, err
	}

	if rd, ok := dec.(PublicResettableReader); ok {
		return wrapResettableDecoder(rd, c.pool), nil
	}

	return dec, nil
}

type MultiDecoder struct {
	m map[rawtopiccommon.Codec]*decoderCreator
}

func NewMultiDecoder() *MultiDecoder {
	md := &MultiDecoder{
		m: make(map[rawtopiccommon.Codec]*decoderCreator),
	}

	md.addDecoder(
		rawtopiccommon.CodecRaw,
		func(input io.Reader) (io.Reader, error) {
			return input, nil
		},
		// Do not use pool for raw codec since it uses identity createFunc.
		// Otherwise input that implements PublicResettableReader will be put to pool.
		false,
	)
	md.addDecoder(
		rawtopiccommon.CodecGzip,
		func(input io.Reader) (io.Reader, error) {
			return gzip.NewReader(input)
		},
		true,
	)

	return md
}

func (d *MultiDecoder) AddDecoder(codec rawtopiccommon.Codec, createFunc PublicCreateDecoderFunc) {
	d.addDecoder(codec, createFunc, true)
}

func (d *MultiDecoder) addDecoder(codec rawtopiccommon.Codec, createFunc PublicCreateDecoderFunc, usePool bool) {
	var pool *resettableDecoderPool
	if usePool {
		pool = newResettableDecoderPool()
	}

	d.m[codec] = &decoderCreator{
		create: createFunc,
		pool:   pool,
	}
}

func (d *MultiDecoder) Decode(codec rawtopiccommon.Codec, input io.Reader) (io.Reader, error) {
	if creator, ok := d.m[codec]; ok {
		return creator.getDecoder(input)
	}

	return nil, xerrors.WithStackTrace(xerrors.Wrap(
		fmt.Errorf("ydb: failed decompress message with codec %v: %w", codec, ErrPublicUnexpectedCodec),
	))
}

type PublicCreateDecoderFunc func(input io.Reader) (io.Reader, error)

// ErrPublicUnexpectedCodec return when try to read message content with unknown codec
var ErrPublicUnexpectedCodec = xerrors.Wrap(errors.New("ydb: unexpected codec"))
