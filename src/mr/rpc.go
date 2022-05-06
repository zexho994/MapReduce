package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

type ApplyTask struct {
	WorkerId        string
	PreTaskType     int8
	PreTaskFileName string
}

type ApplyTaskReply struct {
	TaskType int8
	NumReduce  int
	FilePath string
}

type MapTaskComplete struct {
	FilePath string
}

type MapTaskCompleteReply struct {
}

type ReduceTaskComplete struct {
	workerId string
}

const NO_TASK_TYPE = 0
const MAP_TASK_TYPE = 1
const REDUCE_TASK_TYPE = 2

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
