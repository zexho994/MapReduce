package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

	for {
		req := RpcReq{ReqType: 1}
		rep := RpcRep{}
		call("Master.ApplyTask", &req, &rep)
		log.Println("rpc response.repType = ", rep.RepType)

		// no task and return
		if rep.RepType == 0 {
			return
		}

		if rep.RepType == 1 {
			worker.processMapTask(rep.FilePath, mapf, reducef)
		} else {
			processReduceTask()
		}
	}

}

// process map task
// step1 : read file by filepath
// step2 : split the file content and save to kv[]
// step3 : create intermediate file and store to "workerId/filepath"
// step4 : count the number of occurrences of words
// step5 : append kv[] to intermediate file
func (w *worker) processMapTask(filepath string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.Println("==> start Map() ...  filepath")
	log.Println("fileName = ", filepath)

	// step1
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("cannot open %v", filepath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filepath)
	}
	file.Close()

	// step2
	kvs := []KeyValue{}
	kva := mapf(filepath, string(content))
	kvs = append(kvs, kva...)
	sort.Sort(ByKey(kvs))

	// step3
	if _, err := os.Stat(w.workerPid); errors.Is(err, os.ErrNotExist) {
		log.Println("mkdir ", w.workerPid)
		err := os.Mkdir(w.workerPid, os.ModePerm)
		if err != nil {
			log.Println("mkdir err", err)
			return
		}
	}
	intermediateFileName := w.workerPid + "/" + filepath
	intermediateFile, _ := os.Create(intermediateFileName)
	defer intermediateFile.Close()

	// step4
	data := ""
	for idx := 0; idx < len(kvs); {
		right := idx + 1
		k := kvs[idx].Key
		for right < len(kvs) && kvs[right].Key == k {
			right++
		}
		n := strconv.Itoa(right - idx)
		if right != len(kvs) {
			data = data + k + " " + n + "\n"
		}
		idx = right
	}

	// step5
	ioutil.WriteFile(intermediateFileName, []byte(data), 1024)
	
	log.Println("==> complete Map() ...")
}

func processReduceTask() {
	fmt.Printf("Reduce \n")
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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
