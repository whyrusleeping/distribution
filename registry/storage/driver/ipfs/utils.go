package ipfs

import (
	"io"
)

type countReader struct {
	r io.Reader
	n int64
}

func (cr *countReader) Read(b []byte) (int, error) {
	n, err := cr.r.Read(b)
	cr.n += int64(n)
	return n, err
}
