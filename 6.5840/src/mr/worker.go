package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
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
	args := RequestTaskArgs{WorkerID: os.Getpid()}
	for {
		task := RequestTaskReply{}
		if !call("Coordinator.RequestTask", &args, &task) {
			// In the basic lab the coordinator exits after the job is done.
			return
		}

		var err error
		switch task.TaskType {
		case "map":
			err = runMapTask(task, mapf)
		case "reduce":
			err = runReduceTask(task, reducef)
		case "wait":
			time.Sleep(200 * time.Millisecond)
			continue
		case "exit":
			return
		default:
			log.Printf("unknown task type %q", task.TaskType)
			return
		}
		if err != nil {
			log.Printf("%s task %d failed: %v", task.TaskType, task.TaskID, err)
			removeAttemptOutputs(task)
			return // Another worker can recover the task after its timeout.
		}

		report := ReportTaskArgs{
			TaskType: task.TaskType,
			TaskID:   task.TaskID,
			Attempts: task.Attempts,
		}
		reply := ReportTaskReply{}
		ok := call("Coordinator.ReportTask", &report, &reply)
		// Accepted files have been renamed by the coordinator. A stale attempt
		// can remove only its own unpublished files, never the winning outputs.
		removeAttemptOutputs(task)
		if !ok {
			return
		}
	}
}

func runMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) error {
	if task.NReduce <= 0 {
		return fmt.Errorf("invalid reduce count %d", task.NReduce)
	}
	content, err := ioutil.ReadFile(task.Filename)
	if err != nil {
		return err
	}
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range mapf(task.Filename, string(content)) {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}
	for reduceID, pairs := range buckets {
		name := mapAttemptFilename(task.TaskID, task.Attempts, reduceID)
		if err := writeTaskOutput(name, func(w io.Writer) error {
			enc := json.NewEncoder(w)
			for _, kv := range pairs {
				if err := enc.Encode(&kv); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func runReduceTask(task RequestTaskReply, reducef func(string, []string) string) error {
	var pairs []KeyValue
	for mapID := 0; mapID < task.NMap; mapID++ {
		file, err := os.Open(mapOutputFilename(mapID, task.TaskID))
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return err
			}
			pairs = append(pairs, kv)
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Key < pairs[j].Key })
	return writeTaskOutput(reduceAttemptFilename(task.TaskID, task.Attempts), func(w io.Writer) error {
		for i := 0; i < len(pairs); {
			j := i + 1
			for j < len(pairs) && pairs[j].Key == pairs[i].Key {
				j++
			}
			values := make([]string, 0, j-i)
			for _, kv := range pairs[i:j] {
				values = append(values, kv.Value)
			}
			result := reducef(pairs[i].Key, values)
			if _, err := fmt.Fprintf(w, "%v %v\n", pairs[i].Key, result); err != nil {
				return err
			}
			i = j
		}
		return nil
	})
}

// Write in the same directory so the final rename is atomic.
func writeTaskOutput(name string, write func(io.Writer) error) error {
	file, err := ioutil.TempFile(".", "mr-tmp-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := write(file); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), name)
}

func mapOutputFilename(mapID, reduceID int) string {
	return fmt.Sprintf("mr-%d-%d", mapID, reduceID)
}

func mapAttemptFilename(mapID, attempt, reduceID int) string {
	return fmt.Sprintf("mr-map-%d-%d-%d", mapID, attempt, reduceID)
}

func reduceAttemptFilename(reduceID, attempt int) string {
	return fmt.Sprintf("mr-reduce-%d-%d", reduceID, attempt)
}

func removeAttemptOutputs(task RequestTaskReply) {
	switch task.TaskType {
	case "map":
		for reduceID := 0; reduceID < task.NReduce; reduceID++ {
			os.Remove(mapAttemptFilename(task.TaskID, task.Attempts, reduceID))
		}
	case "reduce":
		os.Remove(reduceAttemptFilename(task.TaskID, task.Attempts))
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
