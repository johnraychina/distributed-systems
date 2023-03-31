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
	FileName    string
	OutFileName string
	TaskType    int
	NReduce     int
}

// WorkerTask : worker id, start time, task file name, task type
type WorkerTask struct {
	Task       Task
	WorkerId   int // unique worker id
	StartTime  time.Time
	TaskStatus int
}

func (t Task) String() string {
	return fmt.Sprintf("Task: FileName=%s, TaskType=%d", t.FileName, t.TaskType)
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
	message string
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
