package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type MapTask struct {
	taskId   int
	fileName string
	nReduce  int
}

type ReduceTask struct {
	taskId  int
	nReduce int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		mapTask, reduceTask := GetTaskForWork()
		if mapTask != nil {
			fmt.Printf("Accepted map task %v\n", mapTask)
			executeMapTask(mapTask, mapf)
			fmt.Printf("Successfully executed map task %v\n", mapTask)
		}
		if reduceTask != nil {
			fmt.Printf("Accepted reduce task %v\n", reduceTask)
			executeReduceTask(reduceTask, reducef)
			fmt.Printf("Successfully executed reduce task %v\n", reduceTask)
		}
		//no task returned, completing
		// fmt.Println("No task to execute. Finishing")
	}
}

func executeMapTask(mapTask *MapTask, mapf func(string, string) []KeyValue) {
	file, err := os.Open(mapTask.fileName)
	if err != nil {
		log.Fatalf("cannot open %v", mapTask)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Println("ERROR")
		return
	}
	file.Close()
	results := mapf(mapTask.fileName, string(content))
	SaveIntermediateFiles(mapTask.taskId, mapTask.nReduce, results)
	SendBackMapResults(mapTask.taskId)
}

func SaveIntermediateFiles(taskId int, nReduce int, kvs []KeyValue) string {
	var intermediateFiles []*os.File
	for i := 0; i < nReduce; i++ {
		file, _ := os.Create(fmt.Sprintf("intermediate-%d-%d.txt", i, taskId))
		intermediateFiles = append(intermediateFiles, file)
	}
	var encoders []*json.Encoder
	for _, file := range intermediateFiles {
		encoders = append(encoders, json.NewEncoder(file))
	}
	for _, kv := range kvs {
		fileN := ihash(kv.Key) % nReduce
		enc := encoders[fileN]
		enc.Encode(&kv)
	}
	for _, file := range intermediateFiles {
		file.Close()
	}
	return ""
}

func executeReduceTask(reduceTask *ReduceTask, reducef func(string, []string) string) {
	kvs := collectEntriesForNReduce(reduceTask.nReduce)
	sort.Sort(ByKey(kvs))
	fileName := runReduceOnValues(kvs, reducef)
	SendBackReduceResults(reduceTask.taskId, fileName)
}

func collectEntriesForNReduce(n int) []KeyValue {
	var kvs []KeyValue
	intermediateFiles, _ := filepath.Glob(fmt.Sprintf("intermediate-%d-*.txt", n))
	for _, fileName := range intermediateFiles {
		file, _ := os.Open(fileName)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	return kvs
}

func runReduceOnValues(kvs []KeyValue, reducef func(string, []string) string) string {
	i := 0
	tmpFile, _ := os.CreateTemp("", "mr-out-*")

	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	tmpFile.Close()
	return tmpFile.Name()
}

func GetTaskForWork() (*MapTask, *ReduceTask) {
	args := AskForTaskRequest{}

	var mapTask *MapTask
	var reduceTask *ReduceTask
	for {
		reply := AskForTaskResponse{}
		call("Coordinator.AskForTask", &args, &reply)
		if reply.MapTask {
			mapTask = &MapTask{taskId: reply.MapTaskId, fileName: reply.MapTaskFilename, nReduce: reply.MapTaskNReduce}
			break
		} else if reply.ReduceTask {
			reduceTask = &ReduceTask{taskId: reply.ReduceTaskId, nReduce: reply.ReduceTaskNReduce}
			break
		} else if reply.Done {
			break
		} else {
			fmt.Println("No task todo. Sleeping")
			time.Sleep(5 * time.Second)
		}
	}
	return mapTask, reduceTask
}

func SendBackMapResults(taskId int) {
	results := TaskResultsRequest{CompletedMapTaskId: taskId}
	call("Coordinator.TaskResults", results, &TaskResultsResponse{})
}

func SendBackReduceResults(taskId int, fileName string) {
	results := TaskResultsRequest{CompletedReduceTaskId: taskId, CompletedReduceTaskFileName: fileName}
	call("Coordinator.TaskResults", results, &TaskResultsResponse{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
