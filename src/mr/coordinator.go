package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.

	files   []string
	nReduce int

	mapJobs    []MapJob
	reduceJobs []ReduceJob

	finished bool
}

type MapJob struct {
	File  string
	JobId int
}

type ReduceJob struct {
	NReduce int
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
	if len(c.mapJobs) > 0 {
		mapJob := c.mapJobs[0]
		c.mapJobs = c.mapJobs[1:]

		reply.MapTask = true
		reply.MapTaskId = fmt.Sprintf("%d", mapJob.JobId)
		reply.MapTaskFilename = mapJob.File
		reply.MapTaskNReduce = c.nReduce
	} else if len(c.reduceJobs) > 0 {
		reduceJob := c.reduceJobs[0]
		c.reduceJobs = c.reduceJobs[1:]

		reply.ReduceTask = true
		reply.ReduceTaskNReduce = reduceJob.NReduce
	}
	return nil
}

func (c *Coordinator) MapTaskResults(args *MapTaskResultsRequest, reply *MapTaskResultsResponse) error {
	return nil
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
	c := Coordinator{files: files, nReduce: nReduce}

	// Your code here.

	c.server()
	for idx, file := range files {
		c.mapJobs = append(c.mapJobs, MapJob{File: file, JobId: idx})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceJobs = append(c.reduceJobs, ReduceJob{NReduce: i})
	}
	return &c
}
