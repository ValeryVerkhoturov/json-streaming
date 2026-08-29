package jsonstreaming

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/goccy/go-json"
)

type Marshaler struct {
	bufSize int
}

type MarshalerOption func(*Marshaler)

func WithMarshalerBufferSize(n int) MarshalerOption {
	return func(m *Marshaler) {
		if n > 0 {
			m.bufSize = n
		}
	}
}

func NewMarshaler(opts ...MarshalerOption) *Marshaler {
	m := &Marshaler{bufSize: DefaultBufferSize}
	for _, o := range opts {
		o(m)
	}
	return m
}

// MarshalStream writes v as a JSON object to w with streaming semantics for
// channel fields.
//
// v must be a struct or pointer to struct. Each exported field is emitted in
// declaration order using its `json:"..."` tag name (falling back to the field
// name). Fields whose kind is chan (with receive direction) are streamed as a
// JSON array — MarshalStream receives from the channel until it closes and
// emits each value as an array element. All other fields go through
// json.Marshal.
//
// Example:
//
//	type Response struct {
//	    Status string     `json:"status"`
//	    Rows   <-chan Row `json:"rows"`
//	    Total  int        `json:"total"`
//	}
//	ch := make(chan Row)
//	go func() { defer close(ch); ch <- Row{ID: 1}; ch <- Row{ID: 2} }()
//	err := jsonstreaming.NewMarshaler().MarshalStream(ctx, w, Response{Status: "ok", Rows: ch, Total: 2})
//	// {"status":"ok","rows":[{"id":1},{"id":2}],"total":2}
//
// The `,omitempty` tag option is honored using reflect.Value.IsZero. A nil
// channel emits an empty array.
func (m *Marshaler) MarshalStream(ctx context.Context, w io.Writer, v any) error {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return fmt.Errorf("jsonstreaming: MarshalStream: nil pointer")
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("jsonstreaming: MarshalStream requires struct, got %s", rv.Kind())
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	sink := &ctxWriter{w: w, ctx: ctx}
	buf := bufio.NewWriterSize(sink, m.bufSize)
	if err := buf.WriteByte('{'); err != nil {
		return err
	}

	t := rv.Type()
	first := true
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, omitempty, skip := parseJSONTag(f)
		if skip {
			continue
		}
		fv := rv.Field(i)
		if omitempty && fv.IsZero() {
			continue
		}

		if !first {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
		}
		first = false

		if err := writeJSONString(buf, name); err != nil {
			return err
		}
		if err := buf.WriteByte(':'); err != nil {
			return err
		}

		if fv.Kind() == reflect.Chan {
			if dir := fv.Type().ChanDir(); dir&reflect.RecvDir == 0 {
				return fmt.Errorf("jsonstreaming: field %q: channel is send-only", f.Name)
			}
			if err := streamChannelArray(ctx, buf, fv); err != nil {
				return err
			}
		} else {
			b, err := json.Marshal(fv.Interface())
			if err != nil {
				return err
			}
			if _, err := buf.Write(b); err != nil {
				return err
			}
		}
	}

	if err := buf.WriteByte('}'); err != nil {
		return err
	}
	return buf.Flush()
}

// MarshalStream is a convenience for NewMarshaler().MarshalStream(ctx, w, v).
func MarshalStream(ctx context.Context, w io.Writer, v any) error {
	return NewMarshaler().MarshalStream(ctx, w, v)
}

func streamChannelArray(ctx context.Context, buf *bufio.Writer, ch reflect.Value) error {
	if err := buf.WriteByte('['); err != nil {
		return err
	}
	if !ch.IsNil() {
		enc := json.NewEncoder(buf)
		cases := []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: ch},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
		}
		first := true
		for {
			idx, val, ok := reflect.Select(cases)
			if idx == 1 {
				return ctx.Err()
			}
			if !ok {
				break
			}
			if !first {
				if err := buf.WriteByte(','); err != nil {
					return err
				}
			}
			first = false
			if err := enc.Encode(val.Interface()); err != nil {
				return err
			}
		}
	}
	return buf.WriteByte(']')
}

// ctxWriter wraps an io.Writer so every Write call is gated by a context.
type ctxWriter struct {
	w   io.Writer
	ctx context.Context
}

func (c *ctxWriter) Write(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.w.Write(p)
}

func parseJSONTag(f reflect.StructField) (name string, omitempty, skip bool) {
	tag := f.Tag.Get("json")
	if tag == "-" {
		return "", false, true
	}
	if tag == "" {
		return f.Name, false, false
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	if name == "" {
		name = f.Name
	}
	for _, p := range parts[1:] {
		if p == "omitempty" {
			omitempty = true
		}
	}
	return name, omitempty, false
}

func writeJSONString(buf *bufio.Writer, s string) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = buf.Write(b)
	return err
}
