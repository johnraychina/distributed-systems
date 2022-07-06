package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	Id      string
	Type    int
	NMap    int
	NReduce int
}

func (t Task) String() string {
	return fmt.Sprintf("Task{ Id:%s, Type:%d, NMap:%d, NReduce:%d}", t.Id, t.Type, t.NMap, t.NReduce)
}

const (
	TaskTypeMap    = 1
	TaskTypeReduce = 2
	TaskTypeWait   = 0
	TaskTypeExit   = -1
)

const (
	TaskStatusInit     = 0
	TaskStatusAccepted = 1
	TaskStatusFinished = 2
)

type AcceptTaskArgs struct {
}

type AcceptTaskReply struct {
	T *Task
}

type FinishTaskArgs struct {
	T *Task
}

type FinishTaskReply struct {
}

type DoneArg struct {
}

type DoneReply struct {
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
