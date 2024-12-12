package topicwriterinternal

import (
	"compress/gzip"
	"io"
)

type gzipEncoder struct {
	*gzip.Writer
}

func (ge *gzipEncoder) Encode(target io.Writer, data []byte) (int, error) {
	ge.Reset(target)

	n, err := ge.Write(data)
	if err != nil {
		return 0, err
	}

	if err := ge.Flush(); err != nil {
		return 0, err
	}

	if err := ge.Close(); err != nil {
		return 0, err
	}

	return n, nil
}

func newGzipEncoder() any {
	return &gzipEncoder{gzip.NewWriter(nil)}
}
