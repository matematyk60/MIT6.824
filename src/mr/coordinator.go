package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	files   []string
	nReduce int

	mu sync.Mutex

	mapJobs    []MapJob
	reduceJobs []ReduceJob

	mapProgress    map[int]MapJob
	reduceProgress map[int]ReduceJob

	jobIdCounter int

	finished bool
}

type MapJob struct {
	JobId int
	File  string
}

type ReduceJob struct {
	JobId   int
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

// TODO: cleanup, use temp files
func (c *Coordinator) AskForTask(args *AskForTaskRequest, reply *AskForTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.mapJobs) > 0 {
		taskId := c.nextJobId()
		mapJob := c.mapJobs[0]
		c.mapJobs = c.mapJobs[1:]
		c.mapProgress[taskId] = mapJob

		reply.MapTask = true
		reply.MapTaskId = taskId
		reply.MapTaskFilename = mapJob.File
		reply.MapTaskNReduce = c.nReduce

		go c.timeoutTask(func() {
			mapJob, ok := c.mapProgress[taskId]
			if ok {
				fmt.Printf("Timing out the map job %d\n", taskId)
				c.mapJobs = append(c.mapJobs, mapJob)
				delete(c.mapProgress, taskId)
			}
		})
	} else if len(c.reduceJobs) > 0 {
		taskId := c.nextJobId()
		reduceJob := c.reduceJobs[0]
		c.reduceJobs = c.reduceJobs[1:]
		c.reduceProgress[taskId] = reduceJob

		reply.ReduceTask = true
		reply.ReduceTaskId = taskId
		reply.ReduceTaskNReduce = reduceJob.NReduce

		go c.timeoutTask(func() {
			reduceJob, ok := c.reduceProgress[taskId]
			if ok {
				fmt.Printf("Timing out the reduce job %d\n", taskId)
				c.reduceJobs = append(c.reduceJobs, reduceJob)
				delete(c.reduceProgress, taskId)
			}
		})
	} else if c.finished {
		reply.Done = true
	}
	return nil
}

func (c *Coordinator) timeoutTask(onTimeout func()) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	onTimeout()
}

func (c *Coordinator) TaskResults(args *TaskResultsRequest, reply *TaskResultsResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case args.CompletedMapTaskId != 0:
		completed := c.mapProgress[args.CompletedMapTaskId]
		fmt.Printf("Successfully completed map task %v\n", completed)
		delete(c.mapProgress, args.CompletedMapTaskId)
		c.considerMapPhaseCompleted()
	case args.CompletedReduceTaskId != 0:
		completed := c.reduceProgress[args.CompletedReduceTaskId]
		oFileName := fmt.Sprintf("mr-out-%d", completed.NReduce)
		err := os.Rename(args.CompletedReduceTaskFileName, oFileName)
		if err != nil {
			return err
		}
		fmt.Printf("Successfully completed reduce task %v\n", completed)
		delete(c.reduceProgress, args.CompletedReduceTaskId)
		c.considerReducePhaseCompleted()
		//
	}
	return nil
}

func (c *Coordinator) considerMapPhaseCompleted() {
	if len(c.mapJobs) == 0 && len(c.mapProgress) == 0 {
		fmt.Println("Map phase is now completed. Starting reduce jobs")
		for i := 0; i < c.nReduce; i++ {
			c.reduceJobs = append(c.reduceJobs, ReduceJob{NReduce: i})
		}
	}
}

func (c *Coordinator) considerReducePhaseCompleted() {
	if len(c.reduceJobs) == 0 && len(c.reduceProgress) == 0 {
		fmt.Println("Reduce phase is now completed. Finishing")
		c.finished = true
	}
}

func (c *Coordinator) nextJobId() int {
	c.jobIdCounter++
	return c.jobIdCounter
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
	c.mu.Lock()
	finished := c.finished
	c.mu.Unlock()
	return finished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce}

	for idx, file := range files {
		c.mapJobs = append(c.mapJobs, MapJob{File: file, JobId: idx})
	}
	c.mapProgress = make(map[int]MapJob)
	c.reduceProgress = make(map[int]ReduceJob)
	// Your code here.

	c.server()
	return &c
}
