package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type worker struct {
	workerPid string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	worker := worker{workerPid: strconv.Itoa(os.Getpid())}
	at := ApplyTask{WorkerId: worker.workerPid}

	for {
		atp := ApplyTaskReply{}
		call("Master.ApplyTask", &at, &atp)

		if atp.TaskType == NO_TASK_TYPE {
			log.Println("all tasks are complete")
			return
		} else if atp.TaskType == MAP_TASK_TYPE {
			log.Printf("receive a map task. filename = %v. nReduce = %v \n", atp.FilePath, atp.NumReduce)
			worker.processMapTask(atp.FilePath, atp.NumReduce, mapf, reducef)
			at = ApplyTask{WorkerId: worker.workerPid, PreTaskType: MAP_TASK_TYPE, PreTaskFileName: atp.FilePath}
		} else if atp.TaskType == REDUCE_TASK_TYPE {
			log.Printf("receive a reduce task. filename = %v \n", atp.FilePath)
			processReduceTask()
			at = ApplyTask{WorkerId: worker.workerPid, PreTaskType: REDUCE_TASK_TYPE}
		} else {
			log.Fatal("atp task type error = ", atp.TaskType)
			return
		}
	}

}

// process map task
// step1 : read file by filepath
// step2 : split the file content and save to kv[]
// step3 : append kv[] to inter file "inter_x" that the x is hash(key)%nReduce
func (w *worker) processMapTask(filepath string, nReduce int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	log.Printf("🧩 start Map(%v, %v).", filepath, nReduce)

	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("cannot open %v", filepath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filepath)
	}
	file.Close()

	kvs := mapf(filepath, string(content))
	log.Printf("kvs length = %v \n", len(kvs))

	fileMap := make(map[string]*os.File, nReduce)
	for _, kv := range kvs {
		interFileBucket := strconv.Itoa(ihash(kv.Key) % nReduce)
		if fileMap[interFileBucket] == nil {
			fileMap[interFileBucket], _ = os.OpenFile("inter_"+interFileBucket+".txt", os.O_APPEND|os.O_WRONLY, 644)
			defer fileMap[interFileBucket].Close()
		}
		line := kv.Key + " " + kv.Value + "\n"
		fileMap[interFileBucket].WriteString(line)
	}

	for _, file := range fileMap {
		file.Close()
	}

	log.Println("🎉 Map() finished.")
	return true
}

func processReduceTask() {
	fmt.Printf("Reduce \n")
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	log.Printf("call %v", rpcname)
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
