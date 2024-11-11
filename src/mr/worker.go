package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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
	jobId    string
	fileName string
	nReduce  int
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
		mapTask := GetFileForWork()
		file, err := os.Open(mapTask.fileName)
		if err != nil {
			log.Fatalf("cannot open %v", mapTask)
		}
		content, err := io.ReadAll(file)
		file.Close()
		results := mapf(mapTask.fileName, string(content))
		SaveIntermediateFiles(mapTask.jobId, mapTask.nReduce, results)
		SendBackMapResults(fmt.Sprintf("testfilename%v", len(results)))
	}
}

func SaveIntermediateFiles(jobId string, nReduce int, kvs []KeyValue) string {
	var intermediateFiles []*os.File
	for i := 0; i < nReduce; i++ {
		file, _ := os.Create(fmt.Sprintf("intermediate-%d-%s.txt", i, jobId))
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

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func GetFileForWork() MapTask {

	// declare an argument structure.
	args := AskForTaskRequest{}

	// fill in the argument(s).

	// declare a reply structure.

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	// ok := call("Coordinator.AskForTask", &args, &reply)
	var task MapTask
	for {
		reply := AskForTaskResponse{}
		call("Coordinator.AskForTask", &args, &reply)
		if reply.NoJob {
			fmt.Println("No task todo. Sleeping")
			time.Sleep(5 * time.Second)
		} else {
			fmt.Println(reply.JobId)
			task = MapTask{reply.JobId, reply.MapTaskFilename, reply.MapTaskNReduce}
			break
		}
	}
	return task
}

func SendBackMapResults(filename string) {
	results := MapTaskResultsRequest{Filename: filename}
	ok := call("Coordinator.MapTaskResults", results, &MapTaskResultsResponse{})
	if ok {
		fmt.Println("Successfully uploaded map results to the coordinator")
	}
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
