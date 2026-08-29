//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

type processMemoryCounters struct {
	CB                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
}

var (
	psapi                  = syscall.NewLazyDLL("psapi.dll")
	procGetProcessMemoryInfo = psapi.NewProc("GetProcessMemoryInfo")
)

func maxRSS() uint64 {
	handle, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0
	}
	var counters processMemoryCounters
	counters.CB = uint32(unsafe.Sizeof(counters))
	ret, _, _ := procGetProcessMemoryInfo.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&counters)),
		uintptr(counters.CB),
	)
	if ret == 0 {
		return 0
	}
	return uint64(counters.PeakWorkingSetSize)
}
