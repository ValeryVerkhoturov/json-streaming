package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"
)

func randStringBytes(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(52)]
	}
	return string(b)
}

func printBenchmarkInfo(fn string, startTime time.Time) {
	var memStats runtime.MemStats
	bToMb := func(b uint64) uint64 { return b / 1024 / 1024 }
	runtime.ReadMemStats(&memStats)
	fmt.Printf("Func: %s \tRSS = %v MB\tAlloc = %v MB\tTotalAlloc = %v MB\tSys = %v MB\tNumGC = %v \tCost = %s\n",
		fn, bToMb(maxRSS()), bToMb(memStats.Alloc), bToMb(memStats.TotalAlloc), bToMb(memStats.Sys), memStats.NumGC, time.Since(startTime))
}
