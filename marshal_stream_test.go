package jsonstreaming_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	jsonstreaming "github.com/ValeryVerkhoturov/json-streaming"
)

func TestMarshalStream(t *testing.T) {
	type resp struct {
		Status string     `json:"status"`
		Rows   <-chan row `json:"rows"`
		Total  int        `json:"total"`
	}
	ch := make(chan row, 3)
	ch <- row{ID: 1, Name: "a"}
	ch <- row{ID: 2, Name: "b"}
	ch <- row{ID: 3, Name: "c"}
	close(ch)

	var buf bytes.Buffer
	if err := jsonstreaming.MarshalStream(context.Background(), &buf, resp{Status: "ok", Rows: ch, Total: 3}); err != nil {
		t.Fatal(err)
	}

	var out struct {
		Status string `json:"status"`
		Rows   []row  `json:"rows"`
		Total  int    `json:"total"`
	}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v (payload=%q)", err, buf.String())
	}
	if out.Status != "ok" || out.Total != 3 || len(out.Rows) != 3 {
		t.Fatalf("unexpected: %+v", out)
	}
	if out.Rows[0] != (row{ID: 1, Name: "a"}) || out.Rows[2] != (row{ID: 3, Name: "c"}) {
		t.Fatalf("rows mismatch: %+v", out.Rows)
	}
}

func TestMarshalStreamMultipleChannels(t *testing.T) {
	type resp struct {
		Users <-chan row `json:"users"`
		Meta  string     `json:"meta"`
		Logs  <-chan row `json:"logs"`
	}
	users := make(chan row, 2)
	users <- row{ID: 1, Name: "u1"}
	users <- row{ID: 2, Name: "u2"}
	close(users)

	logs := make(chan row, 2)
	logs <- row{ID: 10, Name: "l1"}
	logs <- row{ID: 20, Name: "l2"}
	close(logs)

	var buf bytes.Buffer
	if err := jsonstreaming.MarshalStream(context.Background(), &buf, resp{
		Users: users,
		Meta:  "hi",
		Logs:  logs,
	}); err != nil {
		t.Fatal(err)
	}

	var out struct {
		Users []row  `json:"users"`
		Meta  string `json:"meta"`
		Logs  []row  `json:"logs"`
	}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v (payload=%q)", err, buf.String())
	}
	if len(out.Users) != 2 || out.Users[1].Name != "u2" {
		t.Fatalf("users: %+v", out.Users)
	}
	if out.Meta != "hi" {
		t.Fatalf("meta: %q", out.Meta)
	}
	if len(out.Logs) != 2 || out.Logs[0].ID != 10 {
		t.Fatalf("logs: %+v", out.Logs)
	}
}

func TestMarshalStreamNilChannel(t *testing.T) {
	type resp struct {
		Rows <-chan row `json:"rows"`
	}
	var buf bytes.Buffer
	if err := jsonstreaming.MarshalStream(context.Background(), &buf, resp{Rows: nil}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != `{"rows":[]}` {
		t.Fatalf("expected empty array, got %q", got)
	}
}

func TestMarshalStreamTagOptions(t *testing.T) {
	type resp struct {
		Keep    string `json:"keep"`
		Rename  string `json:"renamed"`
		Skip    string `json:"-"`
		Omit    string `json:"omit,omitempty"`
		Present string `json:"present,omitempty"`
	}
	var buf bytes.Buffer
	if err := jsonstreaming.MarshalStream(context.Background(), &buf, resp{Keep: "k", Rename: "r", Skip: "s", Present: "p"}); err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if _, ok := out["Skip"]; ok {
		t.Error(`"-" tag should skip field`)
	}
	if _, ok := out["omit"]; ok {
		t.Error("omitempty on zero value should skip")
	}
	if out["keep"] != "k" || out["renamed"] != "r" || out["present"] != "p" {
		t.Fatalf("unexpected: %+v", out)
	}
}

func TestMarshalStreamRejectsNonStruct(t *testing.T) {
	if err := jsonstreaming.MarshalStream(context.Background(), io.Discard, 42); err == nil {
		t.Fatal("expected error for non-struct")
	}
}

// TestMarshalerWithBufferSize exercises the struct/method form and its
// buffer-size option: a tiny buffer forces multiple flushes to the sink.
func TestMarshalerWithBufferSize(t *testing.T) {
	type resp struct {
		Rows <-chan row `json:"rows"`
	}
	ch := make(chan row, 8)
	for i := 0; i < 8; i++ {
		ch <- row{ID: i, Name: "abcdefghij"}
	}
	close(ch)

	m := jsonstreaming.NewMarshaler(jsonstreaming.WithMarshalerBufferSize(8))
	var buf bytes.Buffer
	if err := m.MarshalStream(context.Background(), &buf, resp{Rows: ch}); err != nil {
		t.Fatal(err)
	}

	var out struct {
		Rows []row `json:"rows"`
	}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v (payload=%q)", err, buf.String())
	}
	if len(out.Rows) != 8 {
		t.Fatalf("expected 8 rows, got %d (%q)", len(out.Rows), buf.String())
	}
}

// TestMarshalStreamCancelsStalledProducer verifies that a stalled producer
// (channel never receives, never closes) is preempted when ctx is cancelled.
// Without ctx wiring this would hang forever.
func TestMarshalStreamCancelsStalledProducer(t *testing.T) {
	type resp struct {
		Rows <-chan row `json:"rows"`
	}
	ch := make(chan row) // no sender, no close — reflect.Recv would block forever
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- jsonstreaming.MarshalStream(ctx, io.Discard, resp{Rows: ch})
	}()

	// Give the goroutine a beat to reach the receive.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("MarshalStream did not return after cancel")
	}
}

// TestMarshalStreamNoGoroutineLeak drives MarshalStream through many happy-path
// cycles and asserts the goroutine count returns to baseline. Catches leaks
// caused by the library spawning workers it forgets to reap.
func TestMarshalStreamNoGoroutineLeak(t *testing.T) {
	type resp struct {
		Rows <-chan row `json:"rows"`
	}
	baseline := runtime.NumGoroutine()

	for i := 0; i < 200; i++ {
		ch := make(chan row, 4)
		for j := 0; j < 4; j++ {
			ch <- row{ID: j}
		}
		close(ch)
		if err := jsonstreaming.MarshalStream(context.Background(), io.Discard, resp{Rows: ch}); err != nil {
			t.Fatal(err)
		}
	}

	// Give the runtime a moment to reap anything transient.
	for i := 0; i < 20; i++ {
		if runtime.NumGoroutine() <= baseline {
			break
		}
		runtime.Gosched()
		time.Sleep(5 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline {
		t.Fatalf("goroutine leak: baseline=%d after=%d", baseline, got)
	}
}

// TestMarshalStreamCancelStopsProducer verifies the pattern the README
// recommends: producer watches ctx.Done() so cancelling MarshalStream
// unblocks and reaps the producer goroutine. If MarshalStream itself retained
// the channel or held a goroutine, the count would drift up.
func TestMarshalStreamCancelStopsProducer(t *testing.T) {
	type resp struct {
		Rows <-chan row `json:"rows"`
	}
	baseline := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan row)

		go func() {
			defer close(ch)
			for j := 0; ; j++ {
				select {
				case <-ctx.Done():
					return
				case ch <- row{ID: j}:
				}
			}
		}()

		// Consume a bit, then cancel mid-stream.
		go func() {
			time.Sleep(2 * time.Millisecond)
			cancel()
		}()

		err := jsonstreaming.MarshalStream(ctx, io.Discard, resp{Rows: ch})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error: %v", err)
		}
		cancel() // idempotent
	}

	for i := 0; i < 40; i++ {
		if runtime.NumGoroutine() <= baseline {
			break
		}
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline+1 {
		t.Fatalf("goroutine leak: baseline=%d after=%d", baseline, got)
	}
}

// TestMarshalStreamCancelsPreCall confirms a cancelled ctx is rejected at
// entry, before any work.
func TestMarshalStreamCancelsPreCall(t *testing.T) {
	type resp struct {
		Rows <-chan row `json:"rows"`
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := jsonstreaming.MarshalStream(ctx, io.Discard, resp{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
