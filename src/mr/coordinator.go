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
	Tasks                    []*Task
	CompletedMapTaskCount    int
	CompletedReduceTaskCount int
	RwMutex                  sync.RWMutex

	MapTaskNum int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AskTask(req *AskTaskReq, resp *AskTaskResp) error {
	c.RwMutex.Lock()
	defer c.RwMutex.Unlock()
	for _, task := range c.Tasks {
		if task.TaskStatus == TaskCreated {
			task.WorkId = req.WorkerId
			task.TaskStatus = TaskIsAssigned
			resp.Task = task
			if task.timer != nil {
				task.timer.Reset(10 * time.Second)
			} else {
				task.timer = time.AfterFunc(10*time.Second, func() {
					c.RwMutex.Lock()
					defer c.RwMutex.Unlock()
					if task.TaskStatus != TaskIsComplete {
						task.TaskStatus = TaskCreated
						task.WorkId = 0
					}
				})
			}
			return nil
		}
	}

	return nil
}

func (c *Coordinator) IsDone(req *DoneReq, resp *DoneResp) error {
	resp.Done = c.Done()
	return nil
}

func (c *Coordinator) UpdateTaskStatus(req *UpdateTaskStatusReq, resp *UpdateTaskStatusResp) error {
	c.RwMutex.Lock()
	defer c.RwMutex.Unlock()
	task := c.Tasks[req.TaskId]
	if task == nil {
		return nil
	}
	if task.WorkId == req.WorkerId && task.TaskStatus == TaskIsAssigned {
		task.TaskStatus = req.TaskStatus
		if task.TaskType == MapType {
			c.CompletedMapTaskCount += 1
		} else {
			c.CompletedReduceTaskCount += 1
		}
	}

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
	c.RwMutex.RLock()
	defer c.RwMutex.RUnlock()
	return c.CompletedMapTaskCount+c.CompletedReduceTaskCount == len(c.Tasks)
}

func (c *Coordinator) CanRun(req CanRunReq, resp *CanRunResp) error {
	c.RwMutex.RLock()
	defer c.RwMutex.RUnlock()
	task := c.Tasks[req.TaskId]
	if task != nil && task.TaskStatus == TaskIsAssigned && task.WorkId == req.WorkerId {
		if c.CompletedMapTaskCount == c.MapTaskNum {
			resp.CanRun = true
		}
	}

	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// init tasks
	c.Tasks = make([]*Task, len(files)+nReduce)
	for i := 0; i < len(files); i++ {
		c.Tasks[i] = &Task{
			TaskId:     i,
			FileName:   files[i],
			NReduce:    nReduce,
			TaskType:   MapType,
			TaskStatus: TaskCreated,
		}
	}
	for i := 0; i < nReduce; i++ {
		task := &Task{
			TaskId:     i + len(files),
			NReduce:    nReduce,
			TaskType:   ReduceType,
			TaskStatus: TaskCreated,
		}
		filenames := make([]string, len(files))
		for j := 0; j < len(files); j++ {
			filenames[j] = fmt.Sprintf("mr-%d-%d", j, i)
		}
		task.FileNames = filenames
		c.Tasks[i+len(files)] = task
	}
	// init nums
	c.MapTaskNum = len(files)
	// fmt.Printf("coordinator: %s", Marshal(&c))
	// start server
	c.server()
	return &c
}
