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

const (
	STATUS_INIT        = "init"
	STATUS_PENDING     = "pending"
	STATUS_IN_PROGRESS = "in_progress"
	STATUS_COMPLETED   = "completed"
	STATUS_FAILED      = "failed"

	taskTimeout = 10 * time.Second

	TASK_TYPE_MAP    = "map"
	TASK_TYPE_REDUCE = "reduce"
)

type MapTask struct {
	ID        int
	Filename  string
	Status    string
	StartTime time.Time
	InitTime  time.Time
	Attempts  int
}

type ReduceTask struct {
	ID        int
	Status    string
	StartTime time.Time
	InitTime  time.Time
	Attempts  int
}

type Coordinator struct {
	mu              sync.Mutex
	MapTasksList    map[int]*MapTask
	ReduceTasksList map[int]*ReduceTask
	NReduce         int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RequestTask is an RPC handler that assigns a task to a worker.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	*reply = RequestTaskReply{
		TaskType: "wait",
		NReduce:  c.NReduce,
		NMap:     len(c.MapTasksList),
	}
	now := time.Now()

	allMapsCompleted := true
	for _, task := range c.MapTasksList {
		if task.Status == STATUS_COMPLETED {
			continue
		}
		allMapsCompleted = false
		if !taskAvailable(task.Status, task.StartTime, now) {
			continue
		}

		task.Status = STATUS_IN_PROGRESS
		task.StartTime = now
		task.Attempts++
		reply.TaskType = TASK_TYPE_MAP
		reply.TaskID = task.ID
		reply.Filename = task.Filename
		reply.Attempts = task.Attempts
		return nil
	}
	if !allMapsCompleted {
		return nil
	}

	allReducesCompleted := true
	for _, task := range c.ReduceTasksList {
		if task.Status == STATUS_COMPLETED {
			continue
		}
		allReducesCompleted = false
		if !taskAvailable(task.Status, task.StartTime, now) {
			continue
		}

		task.Status = STATUS_IN_PROGRESS
		task.StartTime = now
		task.Attempts++
		reply.TaskType = TASK_TYPE_REDUCE
		reply.TaskID = task.ID
		reply.Attempts = task.Attempts
		return nil
	}
	if allReducesCompleted {
		reply.TaskType = "exit"
	}
	return nil
}

func taskAvailable(status string, startedAt, now time.Time) bool {
	switch status {
	case STATUS_INIT, STATUS_PENDING, STATUS_FAILED:
		return true
	case STATUS_IN_PROGRESS:
		return now.Sub(startedAt) >= taskTimeout
	default:
		return false
	}
}

// ReportTask publishes only the current attempt's complete output files.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	*reply = ReportTaskReply{}

	var status *string
	var attempts int
	var sources, destinations []string
	switch args.TaskType {
	case "map":
		task, ok := c.MapTasksList[args.TaskID]
		if !ok {
			return fmt.Errorf("unknown Map task %d", args.TaskID)
		}
		status, attempts = &task.Status, task.Attempts
		for reduceID := 0; reduceID < c.NReduce; reduceID++ {
			sources = append(sources, mapAttemptFilename(task.ID, args.Attempts, reduceID))
			destinations = append(destinations, mapOutputFilename(task.ID, reduceID))
		}
	case "reduce":
		task, ok := c.ReduceTasksList[args.TaskID]
		if !ok {
			return fmt.Errorf("unknown Reduce task %d", args.TaskID)
		}
		status, attempts = &task.Status, task.Attempts
		sources = append(sources, reduceAttemptFilename(task.ID, args.Attempts))
		destinations = append(destinations, fmt.Sprintf("mr-out-%d", task.ID))
	default:
		return fmt.Errorf("unknown task type %q", args.TaskType)
	}

	if attempts != args.Attempts {
		return nil // A newer attempt owns the task; ignore this late result.
	}
	if *status == STATUS_COMPLETED {
		reply.Accepted = true // Retrying the same completion report is harmless.
		return nil
	}
	if *status != STATUS_IN_PROGRESS {
		return nil
	}
	// Check the full set before publishing any files. Reduce cannot start until
	// every Map is marked complete, so it never observes a partial Map result.
	for _, name := range sources {
		info, err := os.Stat(name)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("output %s is not a regular file", name)
		}
	}
	for index, name := range sources {
		if err := os.Rename(name, destinations[index]); err != nil {
			return err
		}
	}
	*status = STATUS_COMPLETED
	reply.Accepted = true
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
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.MapTasksList {
		if task.Status != STATUS_COMPLETED {
			return false
		}
	}
	for _, task := range c.ReduceTasksList {
		if task.Status != STATUS_COMPLETED {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasksList:    make(map[int]*MapTask),
		ReduceTasksList: make(map[int]*ReduceTask),
		NReduce:         nReduce,
	}

	// Your code here.
	for index, filename := range files {
		// Initialize map tasks for each file
		mapTask := MapTask{
			ID:       index,
			Filename: filename,
			Status:   STATUS_INIT,
			InitTime: time.Now(),
		}
		c.MapTasksList[mapTask.ID] = &mapTask
	}

	for id := 0; id < nReduce; id++ {
		c.ReduceTasksList[id] = &ReduceTask{
			ID:       id,
			Status:   STATUS_INIT,
			InitTime: time.Now(),
		}
	}

	c.server()
	return &c
}
