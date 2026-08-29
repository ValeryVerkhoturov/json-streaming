package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"time"

	jsonstreaming "github.com/ValeryVerkhoturov/json-streaming"
)

func benchStreamWriter(row, rowLen, bufSize int) {
	runtime.GC()
	startTime := time.Now()
	fileName := fmt.Sprintf("StreamWriter_r%d.json", row)

	f, err := os.Create(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	// bufio in front of the file so per-SetRow bytes don't hammer the syscall
	// layer on every row.
	bw := bufio.NewWriterSize(f, bufSize)
	sw, err := jsonstreaming.NewStreamWriter(bw, jsonstreaming.WithBufferSize(bufSize))
	if err != nil {
		fmt.Println(err)
		return
	}

	for r := 0; r < row; r++ {
		rec := makeRow(rowLen, r)
		if err := sw.SetRow(&rec); err != nil {
			fmt.Println(err)
			return
		}
	}
	if err := sw.Flush(); err != nil {
		fmt.Println(err)
		return
	}
	if err := bw.Flush(); err != nil {
		fmt.Println(err)
		return
	}
	printBenchmarkInfo(fileName, startTime)
}
