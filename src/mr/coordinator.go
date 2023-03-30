package mr

import (
	"fmt"
	"log"
	"strconv"
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
	// 1.任务并行执行，不能重复处理，一个任务被哪个work处理中，需要标明，用一个struct实现:taskId-workerId-startTime
	//
	//
	// 2.任务可能失败或超时，需要能由其他worker继续处理：任务必须有超时机制，可以判断从何时开始被处理的。
	// 多个任务，用slice报错，执行完成后则置为success，执行超时，空闲的worker可以扫描到，并CAS抢占处理

	// 3.需要能判断整个任务是否执行完成:
	// todo map-reduce任务是一个动态的graph

	// map：file name --> []kv
	// reduce:  key, []values --> key, len(values)

	// 任务编号，任务状态分3个channel(0-未领取，1-已领取，2-执行完成)
	// 初始化任务: files -> MakeCoordinator -> InitMapTask
	// 领取map任务: InitMapTask --(AcceptTask)---> AcceptedMapTask
	// 完成map任务: AcceptedMapTask --(FinishTask)---> InitReduceTask
	// 领取reduce任务: InitReduceTask -> Accept -> AcceptedReduceTask
	// 完成reduce任务: AcceptedReduceTask -> Finish 完成

	//1、每个文件对应一个map任务编号， map任务将一个文件的内容分成单词数组，然后按 Y = ihash(key) % NReduce 映射到对应的reduce任务。
	//中间产生的文件建议命名为 mr-X-Y, 其中X为map任务编号, Y为reduce任务编号.
	//2、每个reduce任务处理对应 mr-*-Y的文件。
	InitMapTask        chan string
	AcceptedMapTask    chan string
	InitReduceTask     chan string
	AcceptedReduceTask chan string
	Files              []string
}

// Your code here -- RPC handlers for the worker to call.

// AcceptTask 领取任务
// 返回等待：when map任务全都已领取，但还有未完成的
// 返回等待：when reduce任务全都已领取，但还有未完成的
// 返回结束：when reduce任务全部完成
// 等待超时重新领取：when map任务或者reduce任务在已领取状态超过 1分钟，重新领取
//todo 线程安全
func (c *Coordinator) AcceptTask(args AcceptTaskArgs, reply *AcceptTaskReply) error {
	// 获取一个可领取的任务
	// 如果需要等待，则返回一个等待类型的任务，这里reduce必须等map任务全部完成才能执行。
	// 如果没有任务了，则返回一个退出类型的任务
	select {
	case id := <-c.InitMapTask:
		reply.T = &Task{Id: id, Type: TaskTypeMap, NMap: c.NMap, NReduce: c.NReduce}
		c.AcceptedMapTask <- id
		return nil
	default: // 没有可领取的map任务，继续走
	}

	// 如果map任务没有全部完成，则需要等待
	// todo 添加超时支持，由于是分布式环境，所以不用 select timer 模式，而是立即判断是否超时，没超时则返回等待标志
	if len(c.AcceptedMapTask) > 0 {
		reply.T = &Task{Type: TaskTypeWait}
		return nil
	}

	// 走到这里说明前面调用了TaskFinished标记任务，全部完成，并生成了新的reduce任务
	// 执行reduce
	select {
	case id := <-c.InitReduceTask:
		reply.T = &Task{Id: id, Type: TaskTypeReduce, NMap: c.NMap, NReduce: c.NReduce}
		c.AcceptedReduceTask <- id
		return nil
	default: // 没有课领取的reduce任务，继续走
	}

	// 如果reduce任务没有全部完成，则需要等待
	if len(c.AcceptedReduceTask) > 0 {
		reply.T = &Task{Type: TaskTypeWait}
		return nil
	}

	// reduce任务全部完成，worker可以退出
	reply.T = &Task{Type: TaskTypeExit}
	return nil
}

// TaskFinished 标记任务已完成
func (c *Coordinator) TaskFinished(args FinishTaskArgs, reply *FinishTaskReply) error {
	// todo 以一种通用的方式表示任务之间的依赖关系: graph

	// 从AcceptedMap移除map任务
	if args.T.Type == TaskTypeMap {
		id := <-c.AcceptedMapTask
		fmt.Println("map-", id, ": finished")

		// 如果map任务全部执行完成，则拆分NReduce个任务
		if len(c.InitMapTask) == 0 && len(c.AcceptedMapTask) == 0 {
			fmt.Println("dispatch reduce tasks:", c.NReduce)
			for i := 0; i < c.NReduce; i++ {
				c.InitReduceTask <- strconv.Itoa(i)
			}
		}
	} else if args.T.Type == TaskTypeReduce {
		id := <-c.AcceptedReduceTask
		fmt.Println("reduce-", id, ": finished")
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
	done := len(c.InitMapTask) == 0 && len(c.AcceptedMapTask) == 0 &&
		len(c.InitReduceTask) == 0 && len(c.AcceptedReduceTask) == 0
	return done
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// init Coordinator
	c.Files = files
	c.NMap = len(files)
	// map task queue, containing the indexes of each file.
	c.InitMapTask = make(chan string, len(files))
	for id, _ := range files {
		c.InitMapTask <- strconv.Itoa(id)
	}

	c.AcceptedMapTask = make(chan string, len(files))
	c.NReduce = nReduce
	c.InitReduceTask = make(chan string, nReduce)
	c.AcceptedReduceTask = make(chan string, nReduce)

	c.server()
	return &c
}
