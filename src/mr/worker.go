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
			doReduce(reducef, reply.Task)
		}

		FinishTask(reply.Task, workerId)
		fmt.Printf("%v  Finished ", time.Now(), reply.Task)
	}
}

func doReduce(reducef func(string, []string) string, reduceTask *Task) {

	file, err := os.Open(reduceTask.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", file)
	}
	scanner := bufio.NewScanner(file)
	defer file.Close()

	var intermediate []KeyValue
	for scanner.Scan() {
		line := scanner.Text()
		col := strings.Split(line, " ")
		intermediate = append(intermediate, KeyValue{Key: col[0], Value: col[1]})
	}

	// 相同的key组成一组，sort，丢给reduce处理
	// being partitioned into NxM buckets.
	sort.Sort(ByKey(intermediate))

	ofile, err := os.Create(reduceTask.OutFileName)
	if err != nil {
		log.Fatalf("error create file %v", reduceTask.OutFileName)
	}

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

// 输入文件名，输出 mr-reduceId
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
	// fixme 同一台启动多个worker，注意避免互相干扰
	intermediateFiles := make([]*os.File, mapTask.NReduce, mapTask.NReduce)
	for r := 0; r < mapTask.NReduce; r++ {
		ofile, _ := os.Create("mr-" + strconv.Itoa(r))
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
