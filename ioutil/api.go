package ioutil

import (
	"context"
	"io"
)

// Wrappers to allow io.Copy streaming functionality that obeys context cancellations and timeouts

// Clients can always do io.Copy themselves but these utility provide a smarter/interruptible streaming copy function
type DFSUtil interface {
	// Returns number of bytes transferred or error encountered
	StreamIntoDFS(ctx context.Context, reader io.Reader, name string, overwrite bool) (int64, error)
	// Returns number of bytes transferred or error encountered
	StreamFromDFS(ctx context.Context, writer io.WriteCloser, name string) (int64, error)
}

type interruptibleCtx struct {
	ctx context.Context
	r   io.Reader
}

func (r *interruptibleCtx) Read(p []byte) (n int, err error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// NewContextAwareReader gets a context-aware io.Reader.
func NewContextAwareReader(ctx context.Context, r io.Reader) io.Reader {
	return &interruptibleCtx{ctx: ctx, r: r}
}
