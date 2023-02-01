package mr

import (
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
	mu   sync.Mutex // 同步代码块
	cond *sync.Cond // 线程间进行通信

	mapFiles     []string
	nMapTask     int // == len(mapFiles)
	nReduceTasks int

	mapTaskFinished   []bool      // 用于判断某个任务是否完成
	mapTaskIssued     []time.Time // map发布所用的时间
	reduceTaskFinised []bool
	reduceTaskIssued  []time.Time

	isDone bool // 用于标记是否所有的reduce都已经完成了
}

// 任务相关处理
func (m *Coordinator) Task(args *GetTaskArgs, reply *GetTaskReply) error {
	//
	m.mu.Lock()
	defer m.mu.Unlock()
	mapTaskFinished := m.mapTaskFinished
	mapTaskIssued := m.mapTaskIssued
	mapFiles := m.mapFiles
	nReduceTasks := m.nReduceTasks
	for {
		mapDone := true // map是否全部做完
		for m, done := range mapTaskFinished { // 去遍历mapTaskFinished中的所有值
			if !done { // 如果等于false代表没有做过
				if mapTaskIssued[m].IsZero() || time.Since(mapTaskIssued[m]).Seconds() > 10 { // 如果这个map没有被发布过或者这个map发布超过了10s，我们就对它进行分配
					reply.TaskType = Map
					reply.MapFile = mapFiles[m]
					reply.TaskNum = m
					reply.NMapTasks = len(mapFiles)
					reply.NReduceTasks = nReduceTasks
					mapTaskIssued[m] = time.Now()
					return nil
				} else {
					mapDone = false
				}
			}
		}
		if !mapDone { // 如果map还在发布中且发布的时间不超时10s，那么我们继续等待
			m.cond.Wait()
		} else {
			break //否则就是map全部完成，可以往下继续完成reduce
		}
	}

	reduceTaskFinised := m.reduceTaskFinised
	reduceTaskIssued := m.reduceTaskIssued

	for {
		reduceDone := true
		for r, done := range reduceTaskFinised {
			if !done {
				if reduceTaskIssued[r].IsZero() || time.Since(reduceTaskIssued[r]).Seconds() > 10 {
					reply.TaskType = Reduce
					reply.TaskNum = r
					reply.NMapTasks = m.nMapTask
					reduceTaskIssued[r] = time.Now()
					return nil
				} else {
					reduceDone = false
				}
			}
		}
		if !reduceDone {
			m.cond.Wait()
		} else {
			break
		}
	}

	reply.TaskType = Done
	m.isDone = true
	return nil
}

func (m *Coordinator) TaskFinished(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args.TaskType {
	case Map:
		m.mapTaskFinished[args.TaskNum] = true
		break
	case Reduce:
		m.reduceTaskFinised[args.TaskNum] = true
		break
	}
	m.cond.Broadcast()
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false
	if c.isDone == true {

		ret = true
		time.Sleep(1*time.Second)
	}
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	//初始化值
	c.cond = sync.NewCond(&c.mu)
	c.mapFiles = files
	c.nMapTask = len(files)
	c.nReduceTasks = nReduce
	c.mapTaskFinished = make([]bool, len(files))
	c.mapTaskIssued = make([]time.Time, len(files))
	c.reduceTaskFinised = make([]bool, nReduce)
	c.reduceTaskIssued = make([]time.Time, nReduce)

	// 建立一个协程，每隔一秒广播一次，通知coordinator有无任务继续执行
	go func() {
		for{
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
	c.server()
	return &c
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
