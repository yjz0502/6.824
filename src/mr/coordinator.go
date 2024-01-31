package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex    // 用于保护并发访问的互斥锁
	mapFiles    []string      // 存储输入文件的列表
	nReduce     int           // Reduce任务的数量
	mapTasks    []*TaskStatus // 存储Map任务的状态信息
	reduceTasks []*TaskStatus // 存储Reduce任务的状态信息
}

// TaskStatus 存储任务的状态信息
type TaskStatus struct {
	TaskID int    // 任务的唯一标识符
	Phase  string // 任务所处的阶段，可能是"Map"或"Reduce"
	//Worker     string    // 执行任务的Worker节点地址
	//StartTime  time.Time // 任务开始执行的时间
	//FinishTime time.Time // 任务完成执行的时间
	State TaskState // 任务的状态，可能是"Idle"、"InProgress"、"Completed"等
}

// TaskState 表示任务的状态
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
	Failed
)

// Your code here -- RPC handlers for the worker to call.
// 在每个 Worker 启动时调用，Master 把待处理 Map 任务分配给 Worker
func (c *Coordinator) SetMap(args *SetMapArgs, reply *SetMapReply) error {
	// 使用互斥锁保护并发访问
	c.mu.Lock()
	defer c.mu.Unlock()
	// 遍历所有的 Map 任务
	for _, mt := range c.mapTasks {
		// 如果任务处于空闲状态，则分配给当前 Worker
		if mt.State == Idle {
			mt.State = InProgress                  // 将任务状态设置为进行中
			reply.WorkId = mt.TaskID               // 返回任务的唯一标识符
			reply.FileName = c.mapFiles[mt.TaskID] // 返回任务对应的文件名
			//log.Printf("--c.SetMap-- MapId=%d is Set,FileName=%v\n", mt.TaskID, c.mapFiles[mt.TaskID])
			return nil
		}
	}
	//log.Println("--c.SetMap-- filed")
	return errors.New("--c.SetMap-- filed")
}

// 在每个 Worker的Map任务完成后向Coordinator发送消息修改MapTask状态
func (c *Coordinator) EndMap(args *EndMapArgs, reply *EndMapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapTasks[args.MapId].State == InProgress {
		c.mapTasks[args.MapId].State = Completed
		return nil
	}
	return errors.New("--c.EndMap-- filed")
}

// 把待处理的Reduce任务分配给Worker
func (c *Coordinator) SetReduce(args *SetReduceArgs, reply *SetReduceReply) error {
	// 使用互斥锁保护并发访问
	c.mu.Lock()
	defer c.mu.Unlock()
	// 遍历所有的 Map 任务,所有Map任务都完成才可以执行Reduce任务
	for _, mt := range c.mapTasks {
		// 如果还有Map任务没有完成，则退出
		if mt.State != Completed {
			return errors.New(fmt.Sprintf("--c.SetReduce-- err:Map%d is InProgress", mt.TaskID))
		}
	}
	// 遍历所有的 Reduce 任务
	for _, mt := range c.reduceTasks {
		// 如果任务处于空闲状态，则分配给当前 Worker
		if mt.State == Idle {
			mt.State = InProgress      // 将任务状态设置为进行中
			reply.ReduceID = mt.TaskID // 返回任务的唯一标识符
			//log.Printf("--c.SetReduce-- ReduceId=%d is Set\n", reply.ReduceID)
			kva := []KeyValue{}
			//读取各个Map任务的输出键值对到Reply
			for i := 0; i < len(c.mapTasks); i++ {
				file, _ := os.Open(fmt.Sprintf("mr-%d-%d", i, reply.ReduceID))
				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			reply.Kva = kva[:]
			return nil
		}
	}

	return errors.New("--c.SetReduce-- filed")
}

// 在每个 Worker的Reduce任务完成后向Coordinator发送消息修改ReduceTask状态
func (c *Coordinator) EndReduce(args *EndReduceArgs, reply *EndReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceTasks[args.ReduceId].State == InProgress {
		c.reduceTasks[args.ReduceId].State = Completed
		return nil
	}
	return errors.New("--c.EndReduce-- filed")
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

	// 启动定时器，每十秒执行一次检测客户端是否存在的函数
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		// 在这里调用检测客户端存在的函数，传入客户端的 ID 或其他信息
		fmt.Println("client123")
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	for {
		ret = true
		for _, v := range c.reduceTasks {
			if v.State != Completed {
				ret = false
				break
			}
		}
		if ret == true {
			break
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		mapFiles:    files,
		nReduce:     nReduce,
		mapTasks:    make([]*TaskStatus, len(files)),
		reduceTasks: make([]*TaskStatus, nReduce),
	}

	// 初始化任务状态
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = &TaskStatus{TaskID: i, Phase: "Map", State: Idle}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &TaskStatus{TaskID: i, Phase: "Reduce", State: Idle}
	}
	//
	// Your code here.

	c.server()
	return c
}
