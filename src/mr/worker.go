package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	workId   int64
	taskType int8
	state    int8
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for !Done() {
		taskResp := CallAskTask()
		if taskResp == nil || taskResp.Task == nil {
			continue
		}
		task := taskResp.Task
		// fmt.Printf("Worker[%d]: receive task:%+v\n", os.Getpid(), Marshal(task))
		if task.TaskType == MapType {
			HandleMapTask(task, mapf)
		} else if task.TaskType == ReduceType {
			HandleReduceTask(task, reducef)
		}
	}

}

func HandleMapTask(task *Task, mapf func(string, string) []KeyValue) {
	intermediate := make([]KeyValue, 0)
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	// create nReduceFile
	fileEncoderArr := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		// TODO: use temp file
		intermediateFilename := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		file, _ = os.Create(intermediateFilename)
		fileEncoderArr[i] = json.NewEncoder(file)
	}
	// hash the keys and write the pair to the file
	for _, pair := range intermediate {
		targetIndex := ihash(pair.Key) % task.NReduce
		if err := fileEncoderArr[targetIndex].Encode(pair); err != nil {
			fmt.Println(err.Error())
			return
		}
	}
	// notify the coordinator that task has complete
	CallUpdateTaskStatus(task.TaskId, TaskIsComplete)
}

func HandleReduceTask(task *Task, reducef func(string, []string) string) {
	WaitTillCanRun(task.TaskId)
	// fmt.Printf("worker[%d]: can run\n", os.Getpid())
	intermediate := make([]KeyValue, 0)
	// reader file into intermediate
	for _, intermediateFilename := range task.FileNames {
		file, _ := os.Open(intermediateFilename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	// fmt.Printf("intermediate's len:[%d]\n", len(intermediate))
	oname := fmt.Sprintf("mr-out-%d", task.TaskId-task.MapTaskNum)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	CallUpdateTaskStatus(task.TaskId, TaskIsComplete)
}

func WaitTillCanRun(taskId int) {
	for {
		if CallCanRun(taskId) {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func Done() bool {
	// prevent frequency request to the coordinator
	time.Sleep(2 * time.Second)
	// declare an argument structure.
	req := DoneReq{}
	// declare a reply structure.
	resp := DoneResp{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.IsDone", &req, &resp)
	if ok {
		return resp.Done
	} else {
		fmt.Printf("call failed!\n")
		return true
	}
}

func CallAskTask() *AskTaskResp {

	// declare an argument structure.
	req := AskTaskReq{}
	req.WorkerId = os.Getpid()
	// declare a reply structure.
	resp := AskTaskResp{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AskTask", &req, &resp)
	if ok {
		return &resp
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

func CallUpdateTaskStatus(taskId int, status int) {
	req := UpdateTaskStatusReq{}
	req.WorkerId = os.Getpid()
	req.TaskId = taskId
	req.TaskStatus = status
	resp := UpdateTaskStatusResp{}

	ok := call("Coordinator.UpdateTaskStatus", &req, &resp)
	if !ok {
		fmt.Printf("[%s]: call failed!\n", "UpdateTaskStatus")
	}
}

func CallCanRun(taskId int) bool {
	req := CanRunReq{}
	req.WorkerId = os.Getpid()
	req.TaskId = taskId
	resp := CanRunResp{}

	ok := call("Coordinator.CanRun", &req, &resp)
	if ok {
		return resp.CanRun
	} else {
		fmt.Printf("call failed!\n")
		return false
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

func Marshal(v interface{}) string {
	res, err := json.Marshal(v)
	if err != nil {
		return ""
	} else {
		return string(res)
	}
}
