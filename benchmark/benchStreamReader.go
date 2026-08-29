package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"time"

	jsonstreaming "github.com/ValeryVerkhoturov/json-streaming"
)

func benchStreamReader(row, bufSize int) {
	runtime.GC()
	startTime := time.Now()
	fileName := fmt.Sprintf("StreamWriter_r%d.json", row)

	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	sr, err := jsonstreaming.NewStreamReader(bufio.NewReaderSize(f, bufSize))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer sr.Close()

	var rec Row
	count := 0
	for sr.Next() {
		if err := sr.Decode(&rec); err != nil {
			fmt.Println(err)
			return
		}
		count++
	}
	if err := sr.Err(); err != nil {
		fmt.Println(err)
		return
	}
	if count != row {
		fmt.Printf("Test Iterator Error: got %d rows, want %d\n", count, row)
		return
	}
	printBenchmarkInfo(fmt.Sprintf("StreamReader_r%d.json", row), startTime)
}
