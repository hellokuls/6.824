package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int
const(
	Map TaskType = 1
	Reduce TaskType = 2
	Done TaskType = 3
)

type GetTaskArgs struct {
}
// 用来请求Master任务，即master分配给worker任务
type GetTaskReply struct {
	TaskType TaskType // 任务的类型

	TaskNum int // 任务的序号

	NReduceTasks int // 需要知道map写入哪个文件

	MapFile string // map需要的文件

	NMapTasks int // 需要知道有多少个中间文件
}

// 定义finish完成后的任务
type FinishedTaskArgs struct {
	TaskType TaskType
	TaskNum int // 用于知道哪个task完成了
}

type FinishedTaskReply struct {
}





// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
