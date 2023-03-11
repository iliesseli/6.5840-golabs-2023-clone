package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	tasks                    []*Task
	id2Task                  map[int]*Task
	completedMapTaskCount    int
	completedReduceTaskCount int
	rwMutex                  sync.RWMutex

	mapTaskNum int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AskTask(req *AskTaskReq, resp *AskTaskResp) error {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	for _, task := range c.tasks {
		if task.TaskStatus == TaskCreated {
			task.WorkId = req.WorkerId
			resp.Task = task
			if task.timer != nil {
				task.timer.Reset(10 * time.Second)
			} else {
				task.timer = time.AfterFunc(10*time.Second, func() {
					c.rwMutex.RLock()
					defer c.rwMutex.RUnlock()
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
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	task := c.id2Task[req.TaskId]
	if task == nil {
		return nil
	}
	if task.WorkId == req.WorkerId && task.TaskStatus == TaskIsAssigned {
		task.TaskStatus = req.TaskStatus
		if task.TaskType == MapType {
			c.completedMapTaskCount += 1
		} else {
			c.completedReduceTaskCount += 1
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
	c.rwMutex.RLock()
	c.rwMutex.RUnlock()
	return c.completedMapTaskCount+c.completedReduceTaskCount == len(c.tasks)
}

func (c *Coordinator) CanRun(req CanRunReq, resp *CanRunResp) error {
	c.rwMutex.RLock()
	c.rwMutex.RUnlock()
	task := c.id2Task[req.TaskId]
	if task != nil && task.TaskStatus == TaskIsAssigned && task.WorkId == req.WorkerId {
		if c.completedMapTaskCount == c.mapTaskNum {
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
	c.tasks = make([]*Task, len(files)+nReduce)
	for i := 0; i < len(files); i++ {
		c.tasks[i] = &Task{
			TaskId:     i,
			FileName:   files[i],
			NReduce:    nReduce,
			TaskType:   MapType,
			TaskStatus: TaskCreated,
		}
	}
	for i := 0; i < nReduce; i++ {
		task := &Task{
			TaskId:     i,
			NReduce:    nReduce,
			TaskType:   ReduceType,
			TaskStatus: TaskCreated,
		}
		filenames := make([]string, len(files))
		for j := 0; j < len(files); j++ {
			filenames[i] = fmt.Sprintf("m-%d-%d", j, i)
		}
		task.FileNames = filenames
		c.tasks[i+len(files)] = task
	}
	// init map index
	c.id2Task = make(map[int]*Task)
	for _, task := range c.tasks {
		c.id2Task[task.TaskId] = task
	}
	// init nums
	c.mapTaskNum = len(files)
	// start server
	c.server()
	return &c
}
