package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"time"
)

// benchUnmarshal is the buffered baseline reader: read the entire file into
// memory then json.Unmarshal into a []Row.
func benchUnmarshal(row int) {
	runtime.GC()
	startTime := time.Now()
	fileName := fmt.Sprintf("Marshal_r%d.json", row)

	b, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	var data []Row
	if err := json.Unmarshal(b, &data); err != nil {
		fmt.Println(err)
		return
	}
	if len(data) != row {
		fmt.Printf("Test Unmarshal Error: got %d rows, want %d\n", len(data), row)
		return
	}
	printBenchmarkInfo(fmt.Sprintf("Unmarshal_r%d.json", row), startTime)
}
