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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		// Your worker implementation here.
		reply := AcceptTask()
		fmt.Println("Accepted task: ", reply.T)

		switch reply.T.Type {
		case TaskTypeMap:
			doMap(mapf, reply.T)
		case TaskTypeReduce:
			doReduce(reducef, reply.T)
		case TaskTypeWait:
			time.Sleep(1 * time.Second)
		case TaskTypeExit:
			return
		}

		fmt.Println("Finish task: ", reply.T)
		TaskFinished(reply)
	}
}

func doReduce(reducef func(string, []string) string, reduceTask *Task) {

	intermediate := []KeyValue{}

	// mr-mapId-reduceId，for 循环处理一个桶
	for mapId := 0; mapId < reduceTask.NMap; mapId++ {
		file, err := os.Open("mr-" + strconv.Itoa(mapId) + "-" + reduceTask.Id)
		if err != nil {
			log.Fatalf("cannot open %v", mapId)
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			col := strings.Split(line, " ")
			kv := KeyValue{Key: col[0], Value: col[1]}
			intermediate = append(intermediate, kv)
		}
	}

	// 相同的key组成一组，sort，丢给reduce处理
	// being partitioned into NxM buckets.
	sort.Sort(ByKey(intermediate))

	outFileName := "mr-out-" + reduceTask.Id
	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatalf("error create file %v", outFileName)
	}

	lastKey := ""
	words := make([]string, 0)
	for i := 0; i < len(intermediate); i++ {
		kv := intermediate[i]
		if kv.Key != lastKey {
			if lastKey != "" {
				// 前面相同的key，先做reduce，写到输出文件中mr-out-reduceId
				wc := reducef(lastKey, words)
				fmt.Fprintf(outFile, "%s %s\n", lastKey, wc)
			}
			// 清空words
			words = make([]string, 0)
		}

		words = append(words, kv.Value)
	}

	//todo
	// 最后再做一次reduce
	wc := reducef(lastKey, words)
	fmt.Fprintf(outFile, "%s %s\n", lastKey, wc)

}

// 输入文件名，输出 mr-mapId-reduceId
func doMap(mapf func(string, string) []KeyValue, mapTask *Task) {
	mapId := mapTask.Id
	file, err := os.Open(mapId)
	if err != nil {
		log.Fatalf("cannot open %v", mapId)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapId)
	}
	file.Close()

	kva := mapf(mapId, string(content))
	intermediate := []KeyValue{}
	intermediate = append(intermediate, kva...)
	// being partitioned into NxM buckets.
	sort.Sort(ByKey(intermediate))

	// 每个文件分10个桶
	intermediateFiles := make([]*os.File, mapTask.NReduce, mapTask.NReduce)
	for r := 0; r < mapTask.NReduce; r++ {
		ofile, _ := os.Create("mr-" + mapId + "-" + strconv.Itoa(r))
		intermediateFiles[r] = ofile
	}
	defer func() {
		for r := 0; r < mapTask.NReduce; r++ {
			intermediateFiles[r].Close()
		}
	}()

	// todo 算子下推，先预处理部分reduce
	// 将kv做哈希分桶，写入对应文件，以便后续reduce处理
	for _, kv := range intermediate {
		r := ihash(kv.Key) % mapTask.NReduce
		fmt.Fprintf(intermediateFiles[r], "%s %s\n", intermediate[r].Key, intermediate[r].Value)
	}
}

// TaskFinished 完成任务后，调用Coordinator.TaskFinished标记任务为"完成"
func TaskFinished(r *AcceptTaskReply) {
	args := FinishTaskArgs{T: r.T}
	reply := FinishTaskReply{}
	call("Coordinator.TaskFinished", &args, &reply)
}

// AcceptTask 调用Coordinator.AcceptTask领取任务，将标记任务为"已领取"
func AcceptTask() *AcceptTaskReply {
	args := AcceptTaskArgs{}
	reply := AcceptTaskReply{}
	call("Coordinator.AcceptTask", &args, &reply)
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
