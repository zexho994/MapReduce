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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RpcReq struct {
	// 1 apply a task
	// 2 map task completed
	// 3 reduce task completed
	ReqType int8
}

type RpcRep struct {
	// 0 no task
	// 1 map task
	// 2 reduce task
	RepType int8

	// when ReqType != map task, the FilePath will be nil
	FilePath string
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
