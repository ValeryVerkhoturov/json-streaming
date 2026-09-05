package jsonstreaming

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/goccy/go-json"
)

// Marshaler configures how MarshalStream buffers output.
type Marshaler struct {
	bufSize int
}

// MarshalerOption customizes a Marshaler at construction time.
type MarshalerOption func(*Marshaler)

// WithMarshalerBufferSize sets the write buffer size used by MarshalStream.
// Values <= 0 are ignored and the default is preserved.
func WithMarshalerBufferSize(n int) MarshalerOption {
	return func(m *Marshaler) {
		if n > 0 {
			m.bufSize = n
		}
	}
}

// NewMarshaler returns a Marshaler configured with the given options.
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
// emits each value as an array element. Struct-typed fields (or pointers to
// struct) whose type transitively contains a channel are marshaled
// recursively with the same streaming semantics; other values go through
// json.Marshal.
//
// Example:
//
//	type Payload struct {
//	    Status string     `json:"status"`
//	    Rows   <-chan Row `json:"rows"`
//	    Total  int        `json:"total"`
//	}
//	type Response struct {
//	    Data Payload `json:"data"`
//	}
//	ch := make(chan Row)
//	go func() { defer close(ch); ch <- Row{ID: 1}; ch <- Row{ID: 2} }()
//	err := jsonstreaming.NewMarshaler().MarshalStream(ctx, w, Response{Data: Payload{Status: "ok", Rows: ch, Total: 2}})
//	// {"data":{"status":"ok","rows":[{"id":1},{"id":2}],"total":2}}
//
// The `,omitempty` and `,omitzero` tag options are honored. Both skip a field
// whose value is the zero value; `,omitzero` additionally invokes a custom
// `IsZero() bool` method when the field type defines one, matching stdlib
// encoding/json (Go 1.24+) semantics. A nil channel emits an empty array.
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
	if err := m.writeStruct(ctx, buf, rv); err != nil {
		return err
	}
	return buf.Flush()
}

// writeStruct emits an rv (a struct value) as a JSON object, streaming any
// channel fields and recursing into struct-typed fields whose type contains
// a channel.
func (m *Marshaler) writeStruct(ctx context.Context, buf *bufio.Writer, rv reflect.Value) error {
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
		name, omitempty, omitzero, skip := parseJSONTag(f)
		if skip {
			continue
		}
		fv := rv.Field(i)
		if omitempty && fv.IsZero() {
			continue
		}
		if omitzero && isZeroValue(fv) {
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
			continue
		}
		if err := m.writeValue(ctx, buf, fv); err != nil {
			return err
		}
	}
	return buf.WriteByte('}')
}

// writeValue writes a non-channel field. If the value is a struct (or pointer
// to struct) whose type transitively contains a channel, it recurses so nested
// channels stream. Everything else falls back to json.Marshal.
func (m *Marshaler) writeValue(ctx context.Context, buf *bufio.Writer, fv reflect.Value) error {
	for fv.Kind() == reflect.Ptr {
		if fv.IsNil() {
			_, err := buf.WriteString("null")
			return err
		}
		fv = fv.Elem()
	}
	if fv.Kind() == reflect.Struct && typeContainsChan(fv.Type()) {
		return m.writeStruct(ctx, buf, fv)
	}
	b, err := json.Marshal(fv.Interface())
	if err != nil {
		return err
	}
	_, err = buf.Write(b)
	return err
}

// containsChanCache memoizes typeContainsChan since MarshalStream is often
// called repeatedly with the same top-level type.
var containsChanCache sync.Map // map[reflect.Type]bool

// typeContainsChan reports whether t is, or transitively holds, a chan through
// struct fields and pointer indirection. Slices, maps, arrays, and interfaces
// are treated as opaque: nested channels reached only through them are not
// streamed (json.Marshal would fail on them, which is the same as before).
func typeContainsChan(t reflect.Type) bool {
	if v, ok := containsChanCache.Load(t); ok {
		return v.(bool)
	}
	result := typeContainsChanSeen(t, map[reflect.Type]bool{})
	containsChanCache.Store(t, result)
	return result
}

func typeContainsChanSeen(t reflect.Type, seen map[reflect.Type]bool) bool {
	if seen[t] {
		return false
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Chan:
		return true
	case reflect.Ptr:
		return typeContainsChanSeen(t.Elem(), seen)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			if typeContainsChanSeen(f.Type, seen) {
				return true
			}
		}
	}
	return false
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

func parseJSONTag(f reflect.StructField) (name string, omitempty, omitzero, skip bool) {
	tag := f.Tag.Get("json")
	if tag == "-" {
		return "", false, false, true
	}
	if tag == "" {
		return f.Name, false, false, false
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	if name == "" {
		name = f.Name
	}
	for _, p := range parts[1:] {
		switch p {
		case "omitempty":
			omitempty = true
		case "omitzero":
			omitzero = true
		}
	}
	return name, omitempty, omitzero, false
}

// isZeroValue mirrors encoding/json's ,omitzero semantics: if the value's type
// (or its addressable pointer receiver) defines `IsZero() bool`, that method
// decides; otherwise reflect.Value.IsZero applies.
func isZeroValue(v reflect.Value) bool {
	if z, ok := v.Interface().(interface{ IsZero() bool }); ok {
		return z.IsZero()
	}
	if v.CanAddr() {
		if z, ok := v.Addr().Interface().(interface{ IsZero() bool }); ok {
			return z.IsZero()
		}
	}
	return v.IsZero()
}

func writeJSONString(buf *bufio.Writer, s string) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = buf.Write(b)
	return err
}
