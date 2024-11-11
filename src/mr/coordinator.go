package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

type Coordinator struct {
	// Your definitions here.

	files     []string
	filesLeft []string
	nReduce   int
	finished  bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForTask(args *AskForTaskRequest, reply *AskForTaskResponse) error {
	if len(c.filesLeft) == 0 {
		reply.NoJob = true
	} else {
		reply.JobId = fmt.Sprintf("%d", len(c.filesLeft))
		reply.MapTaskFilename = c.filesLeft[0]
		reply.MapTaskNReduce = c.nReduce
	}
	return nil
}

func (c *Coordinator) MapTaskResults(args *MapTaskResultsRequest, reply *MapTaskResultsResponse) error {
	fmt.Println(fmt.Sprintf("Got the results in file %v", args))
	c.filesLeft = c.filesLeft[1:]
	if len(c.filesLeft) == 0 {
		// c.finished = true
		c.runReduceJobs()
	}
	return nil
}

func (c *Coordinator) runReduceJobs() {

	// fmt.Println("HELLO")
	// fmt.Println(intermediateFiles)
	for r := 0; r < c.nReduce; r++ {
		kvs := collectEntriesForNReduce(r)
		sort.Sort(ByKey(kvs))
		runReduceOnValues(kvs)
		fmt.Printf("Reduce n of %d successfully completed", r)
	}

	c.finished = true
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

func runReduceOnValues(kvs []KeyValue) {
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		// output := reducef(kvs[i].Key, values)
		fmt.Printf("WOULD reduce %d values for key %s\n", len(values), kvs[i].Key)

		// this is the correct format for each line of Reduce output.
		// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.

	return c.finished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	c.files = files
	c.filesLeft = files
	c.nReduce = nReduce
	c.finished = false
	return &c
}
