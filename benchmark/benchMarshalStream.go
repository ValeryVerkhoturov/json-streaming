package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	jsonstreaming "github.com/ValeryVerkhoturov/json-streaming"
)

// benchMarshalStream exercises the reflection-driven wrapper: a producer
// goroutine feeds rows into a channel, MarshalStream drains and serializes.
func benchMarshalStream(row, rowLen, bufSize int) {
	runtime.GC()
	startTime := time.Now()
	fileName := fmt.Sprintf("MarshalStream_r%d.json", row)

	f, err := os.Create(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, bufSize)

	ch := make(chan Row, 128)
	go func() {
		defer close(ch)
		for r := 0; r < row; r++ {
			ch <- makeRow(rowLen, r)
		}
	}()

	type resp struct {
		Rows <-chan Row `json:"rows"`
	}
	m := jsonstreaming.NewMarshaler(jsonstreaming.WithMarshalerBufferSize(bufSize))
	if err := m.MarshalStream(context.Background(), bw, resp{Rows: ch}); err != nil {
		fmt.Println(err)
		return
	}
	if err := bw.Flush(); err != nil {
		fmt.Println(err)
		return
	}
	printBenchmarkInfo(fileName, startTime)
}
