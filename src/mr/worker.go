package mr

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string, nReduce int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(nReduce))
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	//分配给Worker一个Map任务
	smReply, err := CallSetMap()

	if err != nil {
		log.Fatal("Error calling SetMap:", err)
	}
	// 打开或创建日志文件
	logFile, err := os.Create("WoerkerLogFile" + strconv.Itoa(smReply.WorkId) + ".txt")
	if err != nil {
		log.Fatalf("--Woerker%d-- Error creating log file:%v", smReply.WorkId, err)
	}
	defer logFile.Close()
	// 设置日志输出到文件
	log.SetOutput(logFile)

	//读取文件中的数据到content
	filename := smReply.FileName
	if err != nil {
		log.Fatalf("--Woerker%d -- err:%v", smReply.WorkId, err)
	}
	//select {}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("--Woerker%d-- cannot open %v", smReply.WorkId, filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("--Woerker%d-- cannot read %v", smReply.WorkId, filename)
	}
	file.Close()

	//调用mapa函数
	kva := mapf(filename, string(content))
	//log.Println(kva)

	//建立MapReduce的中间文件，即Map任务的输出
	nReduce := 10
	MidFile := []*os.File{}
	for i := 0; i < nReduce; i++ {
		file, err := os.Create("mr-" + strconv.Itoa(smReply.WorkId) + "-" + strconv.Itoa(i))
		defer file.Close()
		if err != nil {
			log.Fatalf("--Woerker%d-- cannot open %v", smReply.WorkId, filename)
		}
		MidFile = append(MidFile, file)
	}
	log.Printf("--Woerker%d-- Creat All file Success", smReply.WorkId)
	//log.Println("Creat All mr-X-Y file")

	sort.Sort(ByKey(kva))

	//将map任务输出的kva按照ihash的结果写入到对应的中间文件中
	for _, kv := range kva {
		i := ihash(kv.Key, nReduce)
		enc := json.NewEncoder(MidFile[i])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("--Woerker%d-- error encoding and writing to file:%v", smReply.WorkId, err)
		}
	}
	log.Printf("--Woerker%d-- Write MiddleFile file Success", smReply.WorkId)

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	os.CreateTemp()
}

func CallSetMap() (*SetMapReply, error) {
	// 创建带有空值的 SetMapArgs
	args := &SetMapArgs{}
	// 创建带有初始值的 SetMapReply
	reply := &SetMapReply{
		FileName: "",
	}

	// 调用 Coordinator.SetMap 进行 RPC 调用
	ok := call("Coordinator.SetMap", args, reply)

	// 检查 RPC 调用是否成功
	if ok {
		// 打印响应值
		log.Printf("--CallSetMap-- succed reply.WorkId= %d, reply.FileName= %v\n", reply.WorkId, reply.FileName)
		return reply, nil
	} else {
		log.Printf("--CallSetMap--filed！\n")
		return nil, errors.New("--CallSetMap--filed！")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		log.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
	}
}

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
	log.Println("sockname=", sockname)
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		log.Println("--Call-- ", reply)
		return true
	}

	log.Println(err)
	return false
}
