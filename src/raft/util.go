package raft

import "log"

// Debugging
const Debug = 1
const Test = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
}

func TestPrintf(part, format string, a ...interface{}) {
	if Test > 0 {
		log.Printf("[Test-"+part+"]***"+format+"***", a...)
	}
}
