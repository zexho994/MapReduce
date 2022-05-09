package mr

import "os"
import "strconv"

const NO_TASK_TYPE = 0
const MAP_TASK_TYPE = 1
const REDUCE_TASK_TYPE = 2

type ApplyTask struct {
	CommitTaskId   int
	CommitTaskType int
}

type ApplyTaskReply struct {
	TaskId   int
	TaskType int
	FileName      string
	CountOfReduce int
	ReduceIdx     int
}


// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
