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

// Add your RPC definitions here.
type RequestTaskArgs struct {
	WorkerID int
}

type RequestTaskReply struct {
	TaskType string // "map" or "reduce" or "wait" or "exit"
	TaskID   int
	Filename string // for map tasks
	NReduce  int    // for map tasks
	NMap     int    // number of intermediate files each reduce task reads
	Attempts int    // identifies the current assignment when reporting completion
}

type ReportTaskArgs struct {
	TaskType string
	TaskID   int
	Attempts int
}

type ReportTaskReply struct {
	Accepted bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
