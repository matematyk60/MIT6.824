package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
type JobDetails interface {
	MapJob | ReduceJob | NoJobForYou
}

type NoJobForYou struct {
}

type AskForTaskRequest struct {
}

type AskForTaskResponse struct {
	MapTask         bool
	MapTaskId       string
	MapTaskFilename string
	MapTaskNReduce  int

	ReduceTask        bool
	ReduceTaskNReduce int
}

type MapTaskResultsRequest struct {
}

type MapTaskResultsResponse struct {
	/*
		Tasks are expected to be written in files:

		intermediate-<n>-<j>.txt

		here n is the number of reduce bucket
		and j is the id of the job.
	*/
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
