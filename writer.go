package jsonstreaming

import (
	"bufio"
	"context"
	"errors"
	"io"

	"github.com/goccy/go-json"
)

// DefaultBufferSize is the fixed window of memory the writer holds before
// flushing downstream.
const DefaultBufferSize = 1 << 20 // 1 MiB

// StreamWriter serializes a JSON array to an io.Writer one element at a time.
type StreamWriter struct {
	buf      *bufio.Writer
	enc      *json.Encoder
	queryCtx context.Context // non-nil when a field projection is configured
	started  bool
	closed   bool
	count    int
	err      error
}

// NewStreamWriter wraps w and writes the opening bracket of a JSON array.
// The internal buffer size defaults to DefaultBufferSize; override with
// WithBufferSize.
func NewStreamWriter(w io.Writer, opts ...WriterOption) (*StreamWriter, error) {
	cfg := writerConfig{bufSize: DefaultBufferSize}
	for _, o := range opts {
		o(&cfg)
	}
	buf := bufio.NewWriterSize(w, cfg.bufSize)
	enc := json.NewEncoder(buf)

	sw := &StreamWriter{buf: buf, enc: enc}
	if len(cfg.fields) > 0 {
		strs := make([]json.FieldQueryString, len(cfg.fields))
		for i, f := range cfg.fields {
			strs[i] = json.FieldQueryString(f)
		}
		q, err := json.BuildFieldQuery(strs...)
		if err != nil {
			return nil, err
		}
		sw.queryCtx = json.SetFieldQueryToContext(context.Background(), q)
	}

	if _, err := buf.WriteRune('['); err != nil {
		return nil, err
	}
	return sw, nil
}

// WriterOption configures a StreamWriter.
type WriterOption func(*writerConfig)

type writerConfig struct {
	bufSize int
	fields  []string
}

// WithBufferSize overrides the flush buffer size.
func WithBufferSize(n int) WriterOption {
	return func(c *writerConfig) {
		if n > 0 {
			c.bufSize = n
		}
	}
}

// WithFields projects each SetRow value to only the named top-level JSON
// fields; every other field is dropped. Names must match the JSON key (i.e.
// the struct tag, or the field name if untagged).
//
// Given a row {"id":1,"name":"a","secret":"x"} and WithFields("id","name"):
//
//	{"id":1,"name":"a"}
//
// Selection is applied via goccy/go-json's FieldQuery. When set, SetRow uses
// json.MarshalContext under the hood, which allocates a bounded []byte per
// row — trading the writer's zero-alloc guarantee for projection.
func WithFields(names ...string) WriterOption {
	return func(c *writerConfig) { c.fields = names }
}

// SetRow encodes v as the next element of the array.
//
// v may be any type accepted by json.Marshal. Reusing the same pointer across
// calls keeps allocations bounded to the largest single element.
func (sw *StreamWriter) SetRow(v any) error {
	if sw.err != nil {
		return sw.err
	}
	if sw.closed {
		return errors.New("jsonstreaming: writer already flushed")
	}
	if sw.started {
		if err := sw.buf.WriteByte(','); err != nil {
			sw.err = err
			return err
		}
	}
	if sw.queryCtx != nil {
		b, err := json.MarshalContext(sw.queryCtx, v)
		if err != nil {
			sw.err = err
			return err
		}
		if _, err := sw.buf.Write(b); err != nil {
			sw.err = err
			return err
		}
	} else {
		if err := sw.enc.Encode(v); err != nil {
			sw.err = err
			return err
		}
	}
	sw.started = true
	sw.count++
	return nil
}

// Flush writes the closing bracket and flushes the buffer downstream. It is
// safe (and idempotent) to call Flush multiple times; subsequent calls are
// no-ops after the first success.
func (sw *StreamWriter) Flush() error {
	if sw.err != nil {
		return sw.err
	}
	if sw.closed {
		return nil
	}
	if err := sw.buf.WriteByte(']'); err != nil {
		sw.err = err
		return err
	}
	if err := sw.buf.Flush(); err != nil {
		sw.err = err
		return err
	}
	sw.closed = true
	return nil
}

// Abort terminates the stream by appending marker verbatim after the last
// written element (no closing bracket, no separator) and flushing the buffer
// downstream. Intended for the "producer cancelled mid-stream" case: the
// caller decides on a marker (e.g. "Timeout") and downstream consumers see a
// truncated array with a recognizable sentinel.
//
// After Abort the writer is closed; subsequent SetRow calls return an error
// and Flush is a no-op. Abort itself is idempotent.
//
// Example, after two rows:
//
//	sw.Abort("Timeout")  // output: [{...},{...}Timeout
func (sw *StreamWriter) Abort(marker string) error {
	if sw.err != nil {
		return sw.err
	}
	if sw.closed {
		return nil
	}
	if _, err := sw.buf.WriteString(marker); err != nil {
		sw.err = err
		return err
	}
	if err := sw.buf.Flush(); err != nil {
		sw.err = err
		return err
	}
	sw.closed = true
	return nil
}

// Count reports how many elements have been written.
func (sw *StreamWriter) Count() int { return sw.count }
