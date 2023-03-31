package mr

import (
	"fmt"
	"log"
	"strconv"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	NMap    int
	NReduce int

	// 需求：
	// 0.初始化时，通过MakeCoordinator 将任务列表提交给coordinator
	// 1.任务并行执行，不能重复处理，一个任务被哪个work处理中，需要标明，用一个struct实现:taskId-workerId-startTime
	// 2.任务可能失败或超时，需要能由其他worker继续处理：任务必须有超时机制，可以判断从何时开始被处理的。
	// 用一个列表（todo 后续改为 min heap) 保存任务处理状态
	// 任务执行完成后，则置为success，执行超时，空闲的worker可以扫描到，并CAS抢占处理
	// 3.需要能判断整个任务是否执行完成: coordinator的task完成。

	// map：file name --> []kv
	// reduce:  key, []values --> key, len(values)

	//1、每个文件对应一个map任务编号， map任务将一个文件的内容分成单词数组，然后按 Y = ihash(key) % NReduce 映射到对应的reduce任务。
	//中间产生的文件建议命名为 mr-X, 其中X为reduce任务编号.
	//2、每个reduce任务处理对应 mr-X的文件。
	TaskQueue      chan Task     // task file name, task type
	WorkerTaskList []*WorkerTask // worker id, start time, task file name, task type
}

// Your code here -- RPC handlers for the worker to call.

// PullTask 拉取任务
func (c *Coordinator) PullTask(req TaskPullReq, reply *TaskPullReply) error {
	select {
	// pull a task and add to the worker task list, and reply to the worker.
	case task := <-c.TaskQueue:
		fmt.Printf("%v worker(%d) has pull a task %s.\n", time.Now(), req.WorkerId, task)
		for _, e := range c.WorkerTaskList {
			// 未被处理或者 超过10秒，分片给当前请求的worker处理。
			now := time.Now()
			if e.TaskStatus == TaskStatusInit || (e.TaskStatus == TaskStatusProcessing && now.Sub(e.StartTime).Seconds() > 10) {
				e.WorkerId = req.WorkerId
				e.StartTime = now
			}
		}
		reply.Task = &task
	default:
		fmt.Printf("%v worker(%d) tried to pull a task, but no task in queue.\n", time.Now(), req.WorkerId)
		return nil
	}
	return nil
}

// FinishTask 标记任务已完成, todo 后续定时清理该列表
func (c *Coordinator) FinishTask(req TaskFinishReq, reply *TaskFinishReply) error {

	fmt.Printf(" %v worker(%d) has finished %s \n", time.Now(), req.WorkerId, req.Task)

	// remove task from list
	task := req.Task
	for _, e := range c.WorkerTaskList {
		if req.WorkerId == e.WorkerId && e.Task.FileName == task.FileName {

			// 生成下一个任务
			if task.TaskType == TaskTypeMap {
				for i := 0; i < c.NReduce; i++ {
					newTask := Task{
						TaskType: TaskTypeReduce,
						//NReduce:     c.NReduce,
						FileName:    "mr-" + strconv.Itoa(i),
						OutFileName: "mr-out-" + strconv.Itoa(i),
					}
					c.WorkerTaskList = append(c.WorkerTaskList, &WorkerTask{Task: newTask})
					c.TaskQueue <- newTask
				}
			}

			e.TaskStatus = TaskStatusSuccess
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func Done(c *Coordinator) bool {
	// Your code here.
	// todo 并发问题
	for _, e := range c.WorkerTaskList {
		if e.TaskStatus != TaskStatusSuccess {
			return false
		}
	}
	return true
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nMap int, nReduce int) *Coordinator {
	c := Coordinator{}

	// init Coordinator
	c.NMap = nMap
	c.NReduce = nReduce
	c.TaskQueue = make(chan Task, nMap+nReduce)

	// producer: put map task to the queue
	for _, file := range files {
		task := Task{FileName: file, TaskType: TaskTypeMap, NReduce: nReduce}
		c.WorkerTaskList = append(c.WorkerTaskList, &WorkerTask{Task: task})
		c.TaskQueue <- task
	}

	// start coordinator as a rpc server
	c.server()
	return &c
}
