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
	MapTaskQueue   chan Task     // task file name, task type
	MapWorkerTasks []*WorkerTask // worker id, start time, task file name, task type
	MapWaitGroup   sync.WaitGroup

	// 注意：reduce 任务需要等待map任务全部完成才能开始执行
	ReduceTaskQueue   chan Task     // task file name, task type
	ReduceWorkerTasks []*WorkerTask // worker id, start time, task file name, task type
	ReduceWaitGroup   sync.WaitGroup

	Done chan bool // the done channel indicate if the whole task is done
}

// Your code here -- RPC handlers for the worker to call.

// PullTask 拉取任务
func (c *Coordinator) PullTask(req TaskPullReq, reply *TaskPullReply) error {
	select {
	// pull a task and add to the worker task list, and reply to the worker.
	case r := <-c.ReduceTaskQueue:
		reply.Task = c.replyTask(req, r)
		return nil
	case m := <-c.MapTaskQueue:
		reply.Task = c.replyTask(req, m)
		return nil
	default:
		//fmt.Printf("%v worker(%d) tried to pull a task, but no task in queue.\n", time.Now(), req.WorkerId)
		return nil
	}
}

const TimeOutSeconds = 20

func (c *Coordinator) replyTask(req TaskPullReq, task Task) *Task {
	workerTasks := c.MapWorkerTasks
	if task.TaskType == TaskTypeReduce {
		workerTasks = c.ReduceWorkerTasks
	}

	for _, e := range workerTasks {
		// 未被处理或者 超过10秒，分片给当前请求的worker处理。
		now := time.Now()
		if e.TaskStatus == TaskStatusInit {
			e.WorkerId = req.WorkerId
			e.StartTime = now
			e.TaskStatus = TaskStatusProcessing
			fmt.Printf("%v worker(%d) has pull a task %s.\n", time.Now(), req.WorkerId, task)
			return &task
		} else if e.TaskStatus == TaskStatusProcessing && now.Sub(e.StartTime).Seconds() > TimeOutSeconds {
			originalWorkerId := e.WorkerId
			e.WorkerId = req.WorkerId
			e.StartTime = now
			e.TaskStatus = TaskStatusProcessing
			fmt.Printf("%v worker(%d) steal a task %s from worker(%d).\n", time.Now(), req.WorkerId, task, originalWorkerId)
			return &task
		}
	}

	return nil
}

// FinishTask 标记任务已完成
func (c *Coordinator) FinishTask(req TaskFinishReq, reply *TaskFinishReply) error {

	fmt.Printf("%v worker(%d) has finished task: %v \n", time.Now(), req.WorkerId, req.Task)

	// remove task from list
	task := req.Task
	if task.TaskType == TaskTypeMap {
		for _, e := range c.MapWorkerTasks {
			// done
			if e.Task.TaskId == task.TaskId {
				if req.WorkerId == e.WorkerId {
					e.TaskStatus = TaskStatusSuccess
					c.MapWaitGroup.Done()
				} else {
					fmt.Printf("%v The task(%d) of worker(%d) has been stolen by worker(%d) \n", time.Now(), task.TaskId, req.WorkerId, e.WorkerId)
				}
			}
		}

	} else {
		// finish reduce task
		for _, e := range c.ReduceWorkerTasks {
			// done
			if e.Task.TaskId == task.TaskId {
				if req.WorkerId == e.WorkerId {
					e.TaskStatus = TaskStatusSuccess
					c.ReduceWaitGroup.Done()
				} else {
					fmt.Printf("%v The task(%d) of worker(%d) has been stolen by worker(%d) \n", time.Now(), task.TaskId, req.WorkerId, e.WorkerId)
				}
			}
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
	select {
	case <-c.Done:
		return true
	default:
		return false
	}
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nMap int, nReduce int) *Coordinator {
	c := Coordinator{}

	// init Coordinator
	c.NMap = nMap
	c.MapTaskQueue = make(chan Task, nMap)
	c.NReduce = nReduce
	c.ReduceTaskQueue = make(chan Task, nReduce)
	c.Done = make(chan bool, 1)

	// producer: put map task to the queue
	for i, file := range files {
		mTask := Task{TaskId: i, TaskType: TaskTypeMap, FileName: file, NReduce: nReduce}
		c.MapWorkerTasks = append(c.MapWorkerTasks, &WorkerTask{Task: mTask})
		c.MapWaitGroup.Add(1) // add to group before being put/get from queue
		c.MapTaskQueue <- mTask
	}

	// generate reduce task on all map tasks are done
	go func() {
		c.MapWaitGroup.Wait()
		fmt.Printf("%v Map tasks all done.\n", time.Now())

		for reduceId := 0; reduceId < c.NReduce; reduceId++ {
			rTask := Task{TaskType: TaskTypeReduce, TaskId: reduceId}
			c.ReduceWorkerTasks = append(c.ReduceWorkerTasks, &WorkerTask{Task: rTask})
			c.ReduceWaitGroup.Add(1) // add to group before being put/get from queue
			fmt.Printf("%v Enqueue reduce task:%s.\n", time.Now(), rTask)
			c.ReduceTaskQueue <- rTask
		}

		go func() {
			c.ReduceWaitGroup.Wait()
			fmt.Printf("%v Reduce tasks all done.\n", time.Now())

			// when all reduce tasks done, the whole job is done.
			//close(c.ReduceTaskQueue)
			c.Done <- true
			close(c.Done)
		}()
	}()

	// put timeout tasks to queue again
	go func() {
		ticker := time.NewTicker(TimeOutSeconds * time.Second)
		for {
			select {
			case current := <-ticker.C:
				for _, mapWorkerTask := range c.MapWorkerTasks {
					if current.Sub(mapWorkerTask.StartTime).Seconds() > TimeOutSeconds {
						c.MapTaskQueue <- mapWorkerTask.Task
					}
				}

				for _, reduceWorkerTask := range c.ReduceWorkerTasks {
					if current.Sub(reduceWorkerTask.StartTime).Seconds() > TimeOutSeconds {
						c.ReduceTaskQueue <- reduceWorkerTask.Task
					}
				}
			case <-c.Done:
				return
			}
		}
	}()

	// start coordinator as a rpc server
	c.server()

	return &c
}
