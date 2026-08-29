package jsonstreaming

import (
	"errors"
	"fmt"
	"io"

	"github.com/goccy/go-json"
)

// StreamReader pulls elements from a JSON array one at a time.
//
// Usage:
//
//	r, err := jsonstreaming.NewStreamReader(src)
//	if err != nil { return err }
//	defer r.Close()
//	var row Row
//	for r.Next() {
//	    if err := r.Decode(&row); err != nil { return err }
//	    // use row; reuse the same pointer to keep allocations bounded
//	}
//	if err := r.Err(); err != nil { return err }
//
// The reader materializes at most one element at a time. The decoder's
// internal buffer grows only to the size of the largest single element.
type StreamReader struct {
	dec     *json.Decoder
	started bool
	done    bool
	err     error
	index   int
}

// NewStreamReader wraps r and consumes the opening bracket of a JSON array.
// It returns an error if the top-level value is not an array.
func NewStreamReader(r io.Reader) (*StreamReader, error) {
	dec := json.NewDecoder(r)
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("jsonstreaming: read opening token: %w", err)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '[' {
		return nil, fmt.Errorf("jsonstreaming: expected '[', got %v", tok)
	}
	return &StreamReader{dec: dec}, nil
}

// Next advances to the next element. It returns false at end of array or on
// error; check Err to distinguish. Next must be called before the first
// Decode.
func (sr *StreamReader) Next() bool {
	if sr.err != nil || sr.done {
		return false
	}
	if !sr.dec.More() {
		if _, err := sr.dec.Token(); err != nil && !errors.Is(err, io.EOF) {
			sr.err = err
		}
		sr.done = true
		return false
	}
	sr.started = true
	return true
}

// Decode reads the current element into v. v should be a pointer. Reusing the
// same pointer across iterations keeps allocations bounded per element.
func (sr *StreamReader) Decode(v any) error {
	if sr.err != nil {
		return sr.err
	}
	if !sr.started {
		return errors.New("jsonstreaming: Decode called before Next")
	}
	if err := sr.dec.Decode(v); err != nil {
		sr.err = err
		return err
	}
	sr.index++
	return nil
}

// Err reports the first error encountered during iteration, if any.
func (sr *StreamReader) Err() error { return sr.err }

// Close does not close the underlying io.Reader; callers are responsible for
// that. It is provided so the iterator satisfies a Closer-style contract.
func (sr *StreamReader) Close() error { return nil }

// Index reports how many elements have been decoded so far.
func (sr *StreamReader) Index() int { return sr.index }
