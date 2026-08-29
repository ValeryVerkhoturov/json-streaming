package jsonstreaming_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	jsonstreaming "github.com/ValeryVerkhoturov/json-streaming"
)

// row is the shared test fixture used across writer, reader, and marshal
// stream tests (all in package jsonstreaming_test).
type row struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// failingWriter fails after `after` bytes have been written.
type failingWriter struct {
	after   int
	written int
}

var errBoom = errors.New("boom")

func (fw *failingWriter) Write(p []byte) (int, error) {
	if fw.written >= fw.after {
		return 0, errBoom
	}
	room := fw.after - fw.written
	if room > len(p) {
		room = len(p)
	}
	fw.written += room
	if room < len(p) {
		return room, errBoom
	}
	return room, nil
}

// discardWriter is an io.Writer that swallows bytes without allocating, so the
// benchmark measures allocations from the library, not from the sink.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	sw, err := jsonstreaming.NewStreamWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := sw.SetRow(row{ID: i, Name: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.Flush(); err != nil {
		t.Fatal(err)
	}

	sr, err := jsonstreaming.NewStreamReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	var got row
	n := 0
	for sr.Next() {
		if err := sr.Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.ID != n || got.Name != "x" {
			t.Fatalf("row %d: got %+v", n, got)
		}
		n++
	}
	if err := sr.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatalf("expected 100 rows, got %d", n)
	}
}

func TestWithFieldsProjection(t *testing.T) {
	type wide struct {
		ID     int    `json:"id"`
		Name   string `json:"name"`
		Secret string `json:"secret"`
	}
	var buf bytes.Buffer
	sw, err := jsonstreaming.NewStreamWriter(&buf, jsonstreaming.WithFields("id", "name"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sw.SetRow(wide{ID: 1, Name: "a", Secret: "hidden"}); err != nil {
		t.Fatal(err)
	}
	if err := sw.SetRow(wide{ID: 2, Name: "b", Secret: "nope"}); err != nil {
		t.Fatal(err)
	}
	if err := sw.Flush(); err != nil {
		t.Fatal(err)
	}

	var out []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v (payload=%q)", err, buf.String())
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}
	for i, row := range out {
		if _, ok := row["secret"]; ok {
			t.Errorf("row %d: secret leaked: %v", i, row)
		}
		if _, ok := row["id"]; !ok {
			t.Errorf("row %d: missing id: %v", i, row)
		}
		if _, ok := row["name"]; !ok {
			t.Errorf("row %d: missing name: %v", i, row)
		}
	}
}

func TestCountAndIndex(t *testing.T) {
	var buf bytes.Buffer
	sw, err := jsonstreaming.NewStreamWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := sw.SetRow(row{ID: i}); err != nil {
			t.Fatal(err)
		}
	}
	if got := sw.Count(); got != 3 {
		t.Fatalf("Count: expected 3, got %d", got)
	}
	if err := sw.Flush(); err != nil {
		t.Fatal(err)
	}

	sr, err := jsonstreaming.NewStreamReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	var got row
	for sr.Next() {
		if err := sr.Decode(&got); err != nil {
			t.Fatal(err)
		}
	}
	if got := sr.Index(); got != 3 {
		t.Fatalf("Index: expected 3, got %d", got)
	}
}

func TestWithBufferSize(t *testing.T) {
	var buf bytes.Buffer
	// Tiny buffer forces multiple flushes to the underlying writer.
	sw, err := jsonstreaming.NewStreamWriter(&buf, jsonstreaming.WithBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	// Also verify the "invalid size" branch is a no-op (default preserved).
	sw2, err := jsonstreaming.NewStreamWriter(io.Discard, jsonstreaming.WithBufferSize(0))
	if err != nil {
		t.Fatal(err)
	}
	_ = sw2.Flush()

	for i := 0; i < 10; i++ {
		if err := sw.SetRow(row{ID: i, Name: "abcdefghij"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.Flush(); err != nil {
		t.Fatal(err)
	}

	var out []row
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v (payload=%q)", err, buf.String())
	}
	if len(out) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(out))
	}
}

func TestSetRowAfterFlush(t *testing.T) {
	var buf bytes.Buffer
	sw, err := jsonstreaming.NewStreamWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := sw.Flush(); err != nil {
		t.Fatal(err)
	}
	// Flush is idempotent.
	if err := sw.Flush(); err != nil {
		t.Fatalf("second Flush should be no-op, got %v", err)
	}
	if err := sw.SetRow(row{ID: 1}); err == nil {
		t.Fatal("expected error writing after Flush")
	}
}

func TestSetRowAfterError(t *testing.T) {
	// bufSize:1 forces the '[' write to reach the sink immediately, so a
	// subsequent SetRow will trigger a flush that fails.
	fw := &failingWriter{after: 1}
	sw, err := jsonstreaming.NewStreamWriter(fw, jsonstreaming.WithBufferSize(1))
	if err != nil {
		t.Fatalf("expected NewStreamWriter to succeed, got %v", err)
	}
	// First SetRow should fail because the sink refuses further writes.
	firstErr := sw.SetRow(row{ID: 1, Name: strings.Repeat("x", 32)})
	if firstErr == nil {
		t.Fatal("expected first SetRow to fail")
	}
	// A subsequent SetRow returns the sticky error.
	if err := sw.SetRow(row{ID: 2}); err == nil {
		t.Fatal("expected sticky error on second SetRow")
	}
	// Flush also returns the sticky error.
	if err := sw.Flush(); err == nil {
		t.Fatal("expected sticky error on Flush")
	}
}

func TestAbortAppendsMarker(t *testing.T) {
	var buf bytes.Buffer
	sw, err := jsonstreaming.NewStreamWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := sw.SetRow(row{ID: 1, Name: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := sw.SetRow(row{ID: 2, Name: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := sw.Abort("Timeout"); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.HasSuffix(strings.TrimRight(got, "\n"), "Timeout") {
		t.Fatalf("expected trailing Timeout marker, got %q", got)
	}
	if !strings.HasPrefix(got, "[") {
		t.Fatalf("expected leading '[', got %q", got)
	}
	// Abort is idempotent; a second call is a no-op.
	if err := sw.Abort("Timeout"); err != nil {
		t.Fatalf("expected idempotent Abort, got %v", err)
	}
	// After Abort, SetRow errors and Flush is a no-op.
	if err := sw.SetRow(row{ID: 3}); err == nil {
		t.Fatal("expected error on SetRow after Abort")
	}
	if err := sw.Flush(); err != nil {
		t.Fatalf("expected no-op Flush after Abort, got %v", err)
	}
}

func TestAbortAfterCtxCancel(t *testing.T) {
	// Demonstrates the intended usage: caller watches ctx and calls Abort
	// when the context is done.
	ctx, cancel := context.WithCancel(context.Background())
	var buf bytes.Buffer
	sw, err := jsonstreaming.NewStreamWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	source := []row{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}}
	for i, r := range source {
		if i == 2 {
			cancel() // simulate a signal arriving mid-stream
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if err := sw.Abort("Timeout"); err != nil {
				t.Fatal(err)
			}
			break
		}
		if err := sw.SetRow(r); err != nil {
			t.Fatal(err)
		}
	}
	got := buf.String()
	if !strings.HasSuffix(strings.TrimRight(got, "\n"), "Timeout") {
		t.Fatalf("expected trailing Timeout marker, got %q", got)
	}
	if sw.Count() != 2 {
		t.Fatalf("expected 2 rows written, got %d (%q)", sw.Count(), got)
	}
}

func TestOutputIsValidJSONArray(t *testing.T) {
	var buf bytes.Buffer
	sw, _ := jsonstreaming.NewStreamWriter(&buf)
	for i := 0; i < 3; i++ {
		_ = sw.SetRow(row{ID: i, Name: "n"})
	}
	_ = sw.Flush()

	var out []row
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v (payload=%q)", err, buf.String())
	}
	if len(out) != 3 {
		t.Fatalf("expected 3, got %d", len(out))
	}
}

func BenchmarkWriteConstantAlloc(b *testing.B) {
	sw, err := jsonstreaming.NewStreamWriter(discardWriter{})
	if err != nil {
		b.Fatal(err)
	}
	r := row{Name: "item"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ID = i
		if err := sw.SetRow(&r); err != nil {
			b.Fatal(err)
		}
	}
	_ = sw.Flush()
}
