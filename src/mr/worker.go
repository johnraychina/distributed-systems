package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ihash
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Worker
// main/mrworker.go calls this function.
func Worker(workerId int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	fmt.Printf("%v worker(%d) started.\n", time.Now(), workerId)

	for {
		// Your worker implementation here.
		reply := PullTask(workerId)
		if reply.Task == nil {
			time.Sleep(1 * time.Second) // 避免无任务时，疯狂请求RPC
			fmt.Printf("%v No task for now\n", time.Now())
			continue
		}

		fmt.Printf("%v Pulled %s, \n", time.Now(), reply.Task)

		switch reply.Task.TaskType {
		case TaskTypeMap:
			doMap(mapf, reply.Task)
		case TaskTypeReduce:
			// reduce 任务需要等到map任务全部完成才能开始执行：
			//一个方案是worker轮询coordinator请求工作,每次请求之间sleep一下.
			//另一个方案是coordinator中在相关的RPC处理器等待，Go在各自线程中处理RPC的请求, 所以一个RPC等待不会影响其他RPC请求的处理.
			doReduce(reducef, reply.Task)
		}

		//time.Sleep(5 * time.Second) // todo delay一下，方便开多个worker

		FinishTask(reply.Task, workerId)
		fmt.Printf("%v Finished %s \n", time.Now(), reply.Task)
	}
}

func doReduce(reducef func(string, []string) string, reduceTask *Task) {

	outFileName := fmt.Sprintf("mr-out-%d", reduceTask.TaskId)
	ofile, err := os.Create(outFileName)
	if err != nil {
		log.Fatalf("error create file %v", outFileName)
	}
	defer ofile.Close()

	// read mr-*-Y, where Y==reduceTaskId
	// output mr-out-Y
	fileNames := listImmediateFilename(reduceTask.TaskId)
	var intermediate []KeyValue
	for _, fileName := range fileNames {
		fmt.Printf("%v Task:%s reducing file:%v\n", time.Now(), reduceTask, fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		scanner := bufio.NewScanner(file)
		defer file.Close()

		for scanner.Scan() {
			line := scanner.Text()
			col := strings.Split(line, " ")
			intermediate = append(intermediate, KeyValue{Key: col[0], Value: col[1]})
		}
	}
	// 相同的key组成一组，sort，丢给reduce处理
	// being partitioned into NxM buckets.
	sort.Sort(ByKey(intermediate))

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

}

func listImmediateFilename(reduceTaskId int) (fileNames []string) {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		if fileBelongTo(f, reduceTaskId) {
			fileNames = append(fileNames, f.Name())
		}
	}
	return fileNames
}

func fileBelongTo(f os.FileInfo, reduceTaskId int) bool {
	suffix := strconv.Itoa(reduceTaskId)
	return !f.IsDir() && f.Name()[0:2] == "mr" && f.Name()[len(f.Name())-len(suffix):] == suffix
}

// 输入文件名，输出 mr-mapId-reduceId
func doMap(mapf func(string, string) []KeyValue, mapTask *Task) {
	fileName := mapTask.FileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kva := mapf(fileName, string(content))

	var intermediate []KeyValue
	intermediate = append(intermediate, kva...)

	// being partitioned into NxM buckets.
	sort.Sort(ByKey(intermediate))

	// 每个文件分NReduce个桶
	intermediateFiles := make([]*os.File, mapTask.NReduce, mapTask.NReduce)
	for r := 0; r < mapTask.NReduce; r++ {
		ofile, _ := os.Create(fmt.Sprintf("mr-%d-%d", mapTask.TaskId, r))
		intermediateFiles[r] = ofile
	}
	defer func() {
		for r := 0; r < mapTask.NReduce; r++ {
			intermediateFiles[r].Close()
		}
	}()

	// 将kv做哈希分桶，写入对应文件，以便后续reduce处理
	for _, kv := range intermediate {
		r := ihash(kv.Key) % mapTask.NReduce
		fmt.Fprintf(intermediateFiles[r], "%s %s\n", kv.Key, kv.Value)
	}
}

// FinishTask 完成任务后，调用Coordinator.TaskFinished标记任务为"完成"
func FinishTask(task *Task, workerId int) {
	args := TaskFinishReq{Task: task, WorkerId: workerId}
	reply := TaskFinishReply{}
	call("Coordinator.FinishTask", &args, &reply)
}

func PullTask(workerId int) *TaskPullReply {
	req := TaskPullReq{WorkerId: workerId}
	reply := TaskPullReply{}
	call("Coordinator.PullTask", &req, &reply)
	return &reply
}

// call
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
