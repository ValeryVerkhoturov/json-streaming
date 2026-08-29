//go:build !windows

package main

import "syscall"

// maxRSS returns the maximum resident set size of the current process, in
// bytes on Linux and in kilobytes on Darwin/BSD; utils.go converts to MB via
// integer division so the platform difference is visible in the output but
// does not require branching here.
func maxRSS() uint64 {
	var rusage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	return uint64(rusage.Maxrss)
}
