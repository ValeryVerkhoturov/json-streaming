// Benchmark harness for the github.com/ValeryVerkhoturov/json-streaming
// library. Flag-driven, one function per file, per-run stats printed via
// printBenchmarkInfo.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"
)

var (
	funcFlag string
	rowsFlag int
	numFlag  int
	bufFlag  int
)

func init() {
	rand.Seed(time.Now().UnixNano())
	flag.StringVar(&funcFlag, "func", "", "function to benchmark")
	flag.IntVar(&rowsFlag, "rows", 0, "rows to benchmark")
	flag.IntVar(&numFlag, "n", 0, "row (string field) length to benchmark")
	flag.IntVar(&bufFlag, "buf", 1, "flush buffer size in megabytes (streaming benches only)")
}

// bufBytes returns the flush buffer size in bytes, clamped to at least 1 KiB.
func bufBytes() int {
	if bufFlag <= 0 {
		return 1 << 20
	}
	return bufFlag * 1024 * 1024
}

func main() {
	flag.Parse()
	if funcFlag == "" {
		fmt.Println("func is required flag")
		return
	}
	switch funcFlag {
	case "StreamWriter":
		benchStreamWriter(rowsFlag, numFlag, bufBytes())
	case "StreamReader":
		benchStreamReader(rowsFlag, bufBytes())
	case "Marshal":
		benchMarshal(rowsFlag, numFlag)
	case "Unmarshal":
		benchUnmarshal(rowsFlag)
	case "MarshalStream":
		benchMarshalStream(rowsFlag, numFlag, bufBytes())
	default:
		fmt.Println("unsupported benchmark function")
	}
}
