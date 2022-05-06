package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Master struct {
	// Your definitions here.
	files                []string
	filesIdx             int
	nMap                 int
	nReduce              int
	nCompletedMapTask    int
	nCompletedReduceTask int
}

type task struct {
	Type   int
	Status int
}

// Your code here -- RPC handlers for the worker to call.

//
// Process worker rpc request that get task or task completed notice etc.
// step1 : 检查请求是否包含上一个任务完成情况
// step2 : 根据 job 情况分配 task
func (m *Master) ApplyTask(req *ApplyTask, rep *ApplyTaskReply) error {
	log.Printf("worker apply task. workerId = %v. nReduce = %v. nCompleteMap = %v. nCompleteReduce = %v. \n", req.WorkerId, m.nReduce, m.nCompletedMapTask, m.nCompletedReduceTask)

	// 记录上一个task的运行情况
	if req.PreTaskType == MAP_TASK_TYPE {
		log.Println("map task were complete. filepath = ", req.PreTaskFileName)
		m.nCompletedMapTask++
	} else if req.PreTaskType == REDUCE_TASK_TYPE {
		log.Println("reduce task were complete.")
		m.nCompletedReduceTask++
	}

	// 根据job情况分配task
	if m.nCompletedMapTask < m.nMap {
		rep.TaskType = MAP_TASK_TYPE
		rep.NumReduce = m.nReduce
		rep.FilePath = m.files[m.filesIdx]
		m.filesIdx++
	} else if m.nCompletedReduceTask < m.nReduce {
		rep.NumReduce = m.nReduce
		rep.TaskType = REDUCE_TASK_TYPE
	}
	log.Printf("assign task to worker. workerId = %v. taskType = %v. nReduce = %v. filepath = %v.", req.WorkerId, rep.TaskType, rep.NumReduce, rep.FilePath)

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

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
	m := Master{files: files, filesIdx: 0, nMap: len(files), nReduce: nReduce, nCompletedReduceTask: 0}
	log.Printf("create Master. file size = %v. nReduce = %v \n", len(files), nReduce)

	// Your code here.

	// create inter files
	for i := 0; i < nReduce; i++ {
		fileName := "inter_" + strconv.Itoa(i) + ".txt"
		_, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("create file '%v' err. %v", fileName, err)
			return nil
		}
	}

	m.server()
	return &m
}
