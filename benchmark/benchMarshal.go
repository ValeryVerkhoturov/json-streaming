package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"time"
)

// benchMarshal is the buffered baseline: build the full []Row in memory then
// json.Marshal + write it out. Memory usage is bounded by total payload size
// instead of a fixed stream buffer.
func benchMarshal(row, rowLen int) {
	runtime.GC()
	startTime := time.Now()
	fileName := fmt.Sprintf("Marshal_r%d.json", row)

	data := make([]Row, row)
	for r := 0; r < row; r++ {
		data[r] = makeRow(rowLen, r)
	}
	b, err := json.Marshal(data)
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := os.WriteFile(fileName, b, 0644); err != nil {
		fmt.Println(err)
		return
	}
	printBenchmarkInfo(fileName, startTime)
}
