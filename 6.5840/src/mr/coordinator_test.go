package mr

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Avoid starting the shared RPC socket when testing scheduling decisions.
func schedulingCoordinator(nMap, nReduce int) *Coordinator {
	c := &Coordinator{
		MapTasksList:    make(map[int]*MapTask),
		ReduceTasksList: make(map[int]*ReduceTask),
		NReduce:         nReduce,
	}
	for id := 0; id < nMap; id++ {
		c.MapTasksList[id] = &MapTask{
			ID: id, Filename: fmt.Sprintf("input-%d.txt", id), Status: STATUS_INIT,
		}
	}
	for id := 0; id < nReduce; id++ {
		c.ReduceTasksList[id] = &ReduceTask{ID: id, Status: STATUS_INIT}
	}
	return c
}

func TestRequestTaskConcurrentAssignments(t *testing.T) {
	c := schedulingCoordinator(5, 10)
	replies := make(chan RequestTaskReply, 20)
	var requests sync.WaitGroup
	for worker := 0; worker < 20; worker++ {
		requests.Add(1)
		go func(id int) {
			defer requests.Done()
			reply := RequestTaskReply{}
			if err := c.RequestTask(&RequestTaskArgs{WorkerID: id}, &reply); err != nil {
				t.Errorf("request: %v", err)
			}
			replies <- reply
		}(worker)
	}
	requests.Wait()
	close(replies)

	assigned := make(map[int]bool)
	waits := 0
	for reply := range replies {
		if reply.NMap != 5 || reply.NReduce != 10 {
			t.Fatalf("incorrect job dimensions: %+v", reply)
		}
		switch reply.TaskType {
		case "map":
			if assigned[reply.TaskID] {
				t.Fatalf("Map %d assigned twice without timeout", reply.TaskID)
			}
			assigned[reply.TaskID] = true
			if reply.Filename != fmt.Sprintf("input-%d.txt", reply.TaskID) || reply.Attempts != 1 {
				t.Fatalf("incorrect Map assignment: %+v", reply)
			}
		case "wait":
			waits++
		default:
			t.Fatalf("task assigned before Map completion: %+v", reply)
		}
	}
	if len(assigned) != 5 || waits != 15 {
		t.Fatalf("got %d Maps and %d waits, want 5 and 15", len(assigned), waits)
	}
}

func TestRequestTaskStages(t *testing.T) {
	c := schedulingCoordinator(2, 2)
	c.MapTasksList[0].Status = STATUS_COMPLETED
	c.MapTasksList[1].Status = STATUS_IN_PROGRESS
	c.MapTasksList[1].StartTime = time.Now()
	reply := RequestTaskReply{Filename: "stale.txt", Attempts: 99}
	if err := c.RequestTask(&RequestTaskArgs{}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.TaskType != "wait" || reply.Filename != "" || reply.Attempts != 0 {
		t.Fatalf("expected wait with cleared task fields: %+v", reply)
	}

	c.MapTasksList[1].Status = STATUS_COMPLETED
	seen := make(map[int]bool)
	for i := 0; i < 2; i++ {
		if err := c.RequestTask(&RequestTaskArgs{}, &reply); err != nil {
			t.Fatal(err)
		}
		if reply.TaskType != "reduce" || seen[reply.TaskID] || reply.Attempts != 1 {
			t.Fatalf("expected distinct Reduce assignment: %+v", reply)
		}
		seen[reply.TaskID] = true
	}
	c.RequestTask(&RequestTaskArgs{}, &reply)
	if reply.TaskType != "wait" {
		t.Fatalf("expected wait while Reduces run: %+v", reply)
	}
	for _, task := range c.ReduceTasksList {
		task.Status = STATUS_COMPLETED
	}
	c.RequestTask(&RequestTaskArgs{}, &reply)
	if reply.TaskType != "exit" {
		t.Fatalf("expected exit after all Reduces complete: %+v", reply)
	}
}

func TestRequestTaskRetries(t *testing.T) {
	for _, taskType := range []string{"map", "reduce"} {
		t.Run(taskType, func(t *testing.T) {
			c := schedulingCoordinator(1, 1)
			if taskType == "reduce" {
				c.MapTasksList[0].Status = STATUS_COMPLETED
			}
			reply := RequestTaskReply{}
			c.RequestTask(&RequestTaskArgs{WorkerID: 1}, &reply)
			c.RequestTask(&RequestTaskArgs{WorkerID: 2}, &reply)
			if reply.TaskType != "wait" {
				t.Fatalf("task retried before timeout: %+v", reply)
			}
			if taskType == "map" {
				c.MapTasksList[0].StartTime = time.Now().Add(-11 * time.Second)
			} else {
				c.ReduceTasksList[0].StartTime = time.Now().Add(-11 * time.Second)
			}
			before := time.Now()
			c.RequestTask(&RequestTaskArgs{WorkerID: 2}, &reply)
			if reply.TaskType != taskType || reply.TaskID != 0 || reply.Attempts != 2 {
				t.Fatalf("expected retry of timed-out task: %+v", reply)
			}
			startedAt := c.MapTasksList[0].StartTime
			if taskType == "reduce" {
				startedAt = c.ReduceTasksList[0].StartTime
			}
			if startedAt.Before(before) {
				t.Fatal("retry did not renew task start time")
			}
			c.RequestTask(&RequestTaskArgs{WorkerID: 3}, &reply)
			if reply.TaskType != "wait" {
				t.Fatalf("retry immediately assigned again: %+v", reply)
			}
		})
	}
}
