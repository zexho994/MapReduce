package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	files       []string
	nReduce     int
	mapTasks    *taskPoll
	reduceTasks *taskPoll
}

// ApplyTask
// Process worker rpc request that get task or task completed notice etc.
// step1 : 检查请求是否包含上一个任务完成情况
// step2 : 根据 job 情况分配 task
func (m *Master) ApplyTask(req *ApplyTask, rep *ApplyTaskReply) error {
	log.Printf("worker apply task. commitTaskId = %v. commitTaskType = %v \n", req.CommitTaskId, req.CommitTaskType)

	// 记录上一个task的运行情况
	if req.CommitTaskType == MAP_TASK_TYPE {
		m.mapTasks.Commit(req.CommitTaskId)
	} else if req.CommitTaskType == REDUCE_TASK_TYPE {
		m.reduceTasks.Commit(req.CommitTaskId)
	}

	// 根据job情况分配task
	if t := m.mapTasks.GetTask(); t != nil {
		rep.TaskId = t.Id
		rep.TaskType = MAP_TASK_TYPE
		rep.FileName = m.files[t.Id]
		rep.CountOfReduce = m.reduceTasks.Size()
	} else if t := m.reduceTasks.GetTask(); t != nil {
		rep.TaskId = t.Id
		rep.TaskType = REDUCE_TASK_TYPE
		rep.CountOfReduce = m.reduceTasks.Size()
		rep.ReduceIdx = t.Id
	}

	return nil
}

// server start a thread that listens for RPCs from worker.go
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

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.reduceTasks.IsAllCompleted()
}

// MakeMaster
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files, nReduce: nReduce, mapTasks: TaskPoll(len(files)), reduceTasks: TaskPoll(nReduce)}
	log.Printf("create Master. file size = %v. nReduce = %v mapTasks len = %v \n", len(files), nReduce, m.mapTasks.Size())
	m.server()
	return &m
}
