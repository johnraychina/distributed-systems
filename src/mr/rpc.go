package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Task : task file name, task type
type Task struct {
	TaskType int
	TaskId   int
	NReduce  int
	FileName string
}

// WorkerTask : worker id, start time, task file name, task type
type WorkerTask struct {
	Task       Task
	WorkerId   int // unique worker id
	StartTime  time.Time
	TaskStatus int
}

func (t Task) String() string {
	taskTypeStr := "Unknown"
	switch t.TaskType {
	case TaskTypeMap:
		taskTypeStr = "Map"
	case TaskTypeReduce:
		taskTypeStr = "Reduce"
	}
	return fmt.Sprintf("Task: TaskId=%d, TaskType=%s, FileName=%s", t.TaskId, taskTypeStr, t.FileName)
}

const (
	TaskTypeMap    = 1
	TaskTypeReduce = 2
)

const (
	TaskStatusInit       = 0
	TaskStatusProcessing = 1
	TaskStatusSuccess    = 2
)

type TaskPullReq struct {
	WorkerId int
}

type TaskPullReply struct {
	Task *Task
}

type TaskFinishReq struct {
	WorkerId int
	Task     *Task
}

type TaskFinishReply struct {
	Message string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
