package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	MapType    = 0
	ReduceType = 1
)

const (
	WorkerOffline    = 0
	WorkerIdle       = 1
	WorkerInProgress = 2
	WorkerCompleted  = 3
)

const (
	TaskCreated    = 0
	TaskIsAssigned = 1
	TaskIsComplete = 2
)

type Task struct {
	TaskId     int
	WorkId     int
	FileName   string   // for map type
	FileNames  []string // for reduce type
	NReduce    int
	TaskType   int
	TaskStatus int

	timer *time.Timer
}

type AskTaskReq struct {
	WorkerId int
}

type AskTaskResp struct {
	Task *Task
}

type DoneReq struct {
}

type DoneResp struct {
	Done bool
}

type UpdateTaskStatusReq struct {
	WorkerId   int
	TaskId     int
	TaskStatus int
}

type UpdateTaskStatusResp struct {
}

type CanRunReq struct {
	WorkerId int
	TaskId   int
}

type CanRunResp struct {
	CanRun bool
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
