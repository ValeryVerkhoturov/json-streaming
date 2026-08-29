package jsonstreaming_test

import (
	"io"
	"strings"
	"testing"

	jsonstreaming "github.com/ValeryVerkhoturov/json-streaming"
)

func TestEmptyArray(t *testing.T) {
	sr, err := jsonstreaming.NewStreamReader(strings.NewReader("[]"))
	if err != nil {
		t.Fatal(err)
	}
	if sr.Next() {
		t.Fatal("expected no elements")
	}
	if err := sr.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectsNonArray(t *testing.T) {
	if _, err := jsonstreaming.NewStreamReader(strings.NewReader(`{"x":1}`)); err == nil {
		t.Fatal("expected error for object input")
	}
}

func TestDecodeBeforeNext(t *testing.T) {
	sr, err := jsonstreaming.NewStreamReader(strings.NewReader(`[{"id":1}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	var got row
	if err := sr.Decode(&got); err == nil {
		t.Fatal("expected error: Decode before Next")
	}
}

func TestDecodeAfterError(t *testing.T) {
	// Malformed element after '[' — Decode surfaces the error, then a
	// subsequent Decode returns the sticky error.
	sr, err := jsonstreaming.NewStreamReader(strings.NewReader(`[not-json]`))
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	if !sr.Next() {
		t.Fatal("expected Next to advance to the malformed element")
	}
	var got row
	if err := sr.Decode(&got); err == nil {
		t.Fatal("expected decode error")
	}
	// Sticky.
	if err := sr.Decode(&got); err == nil {
		t.Fatal("expected sticky decode error")
	}
	if sr.Next() {
		t.Fatal("expected Next to return false after error")
	}
}

// repeatingReader emits a fixed JSON array N times over without holding it in
// memory, so BenchmarkReadConstantAlloc measures decoder alloc-per-row on a
// long stream.
type repeatingReader struct {
	elem      string
	remaining int
	emitted   int
	buf       string
	pos       int
	closed    bool
}

func newRepeatingReader(n int) *repeatingReader {
	return &repeatingReader{
		elem:      `{"id":1,"name":"x"}`,
		remaining: n,
		buf:       "[",
	}
}

func (r *repeatingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.buf) {
		switch {
		case r.remaining > 0:
			if r.emitted == 0 {
				r.buf = r.elem
			} else {
				r.buf = "," + r.elem
			}
			r.emitted++
			r.remaining--
			r.pos = 0
		case !r.closed:
			r.buf = "]"
			r.closed = true
			r.pos = 0
		default:
			return 0, io.EOF
		}
	}
	n := copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}

func BenchmarkReadConstantAlloc(b *testing.B) {
	src := newRepeatingReader(b.N)
	sr, err := jsonstreaming.NewStreamReader(src)
	if err != nil {
		b.Fatal(err)
	}
	var got row
	b.ReportAllocs()
	b.ResetTimer()
	for sr.Next() {
		if err := sr.Decode(&got); err != nil {
			b.Fatal(err)
		}
	}
}
