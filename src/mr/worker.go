package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	os "os"
	"sort"
	"strconv"
	"strings"
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
	at := ApplyTask{}

	for {
		atp := ApplyTaskReply{}
		call("Master.ApplyTask", &at, &atp)

		if atp.TaskType == NO_TASK_TYPE {
			log.Println("all tasks are complete")
			return
		} else if atp.TaskType == MAP_TASK_TYPE {
			log.Printf("receive a map task. taskId = %v. filename = %v. nReduce = %v \n", atp.TaskId, atp.FileName, atp.CountOfReduce)
			worker.processMapTask(atp.FileName, atp.CountOfReduce, mapf, reducef)
			at.CommitTaskType = MAP_TASK_TYPE
			at.CommitTaskId = atp.TaskId
		} else if atp.TaskType == REDUCE_TASK_TYPE {
			log.Printf("receive a reduce task. taskId = %v. reduceIdx = %v \n", atp.TaskId, atp.ReduceIdx)
			worker.processReduceTask(atp.ReduceIdx, reducef)
			at.CommitTaskType = REDUCE_TASK_TYPE
			at.CommitTaskId = atp.TaskId
		} else {
			log.Fatal("atp task type error = ", atp.TaskType)
			return
		}
	}
}

// process map task
// step1 : read file by filepath
// step2 : split the file content and save to kv[]
// step3 : append kv[] to inter file "mr-inter-x" that the x is hash(key)%nReduce
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

	fileMap := make(map[int]*os.File, nReduce)
	for _, kv := range kvs {
		interFileBucket := getBucketIdx(kv.Key, nReduce)
		if fileMap[interFileBucket] == nil {
			fileMap[interFileBucket], _ = os.OpenFile(getIntermediateFileName(strconv.Itoa(getBucketIdx(kv.Key, nReduce))), os.O_APPEND|os.O_WRONLY, os.ModePerm)
			defer fileMap[interFileBucket].Close()
		}
		line := kv.Key + " " + kv.Value + "\n"
		fileMap[interFileBucket].WriteString(line)
	}

	log.Println("🎉 Map() finished.")
	return true
}

// processReduceTask
// step1: read intermediate file by reduceIdx
// step2: count the number of word
// step3: write the data of step2 to mr-out-{reduceIdx}
func (w *worker) processReduceTask(reduceIdx int, reducef func(string, []string) string) {
	log.Printf("🧱 start Reduce(%v) \n", reduceIdx)

	// read from intermediate file 'mr-inter-{reduceIdx}'
	interFileName := getIntermediateFileName(strconv.Itoa(reduceIdx))
	interFile, err := os.OpenFile(interFileName, os.O_RDONLY, os.ModePerm)
	log.Printf("get intermediate file %v \n", interFile.Name())
	if err != nil {
		log.Fatalf("open intermediate file error. filename = %v. err = %v \n", interFileName, err)
	}

	// count the number of word
	var interKV []KeyValue
	fs := bufio.NewScanner(interFile)
	for fs.Scan() {
		strLine := fs.Text()
		strLineSplit := strings.Split(strLine, " ")
		interKV = append(interKV, KeyValue{strLineSplit[0], strLineSplit[1]})
	}
	interFile.Close()
	sort.Sort(ByKey(interKV))
	log.Printf("interKV len = %v \n", len(interKV))

	// write kv to mr-out-{reduceIdx}
	reduceFileName := "mr-out-" + strconv.Itoa(reduceIdx)
	reduceFile, _ := os.OpenFile(reduceFileName, os.O_APPEND|os.O_WRONLY, os.ModePerm)

	i := 0
	for i < len(interKV) {
		var values []string
		j := i + 1
		for j < len(interKV) && interKV[j].Key == interKV[i].Key {
			j++
		}
		for k := i; k < j; k++ {
			values = append(values, interKV[k].Value)
		}
		count := reducef(interKV[i].Key, values)
		reduceFile.WriteString(interKV[i].Key + " " + count + "\n")

		i = j
	}

	reduceFile.Close()
	log.Println("🎉 Reduce() finished.")
}

func getBucketIdx(k string, bucketSize int) int {
	return ihash(k) % bucketSize
}

func getIntermediateFileName(k string) string {
	return "mr-inter-" + k
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//log.Printf("call %v", rpcname)
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
