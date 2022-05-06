package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Your definitions here.
	files            []string
	filesIdx         int
	nReduce          int
	nCompletedReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// Process worker rpc request that get task or task completed notice etc.
func (m *Master) ApplyTask(req *RpcReq, rep *RpcRep) error {
	fmt.Printf("worker apply task. req %v \n", req.ReqType)

	//version1
	if(m.filesIdx < len(m.files)){
		rep.RepType = 1
		rep.FilePath = m.files[m.filesIdx]
		m.filesIdx++
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files, filesIdx: 0, nReduce: nReduce, nCompletedReduce: 0}
	fmt.Printf("create Master. file size = %v. nReduce = %v \n", len(files), nReduce)

	// Your code here.

	m.server()
	return &m
}
