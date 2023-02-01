package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 向master发送请求，来请求得到一个任务
	CallTask(mapf, reducef)

	// 需要将map之后产生的结果保存在文件中

}

// 向master发送请求，来请求得到一个任务
func CallTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		call("Coordinator.Task", &args, &reply)

		switch reply.TaskType {
		case Map:
			executeMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf) //去执行map操作
			break
		case Reduce:
			executeReduce(reply.TaskNum, reply.NMapTasks, reducef)
			break
		case Done:
			os.Exit(0)
		}
		finishedArgs := FinishedTaskArgs{}
		finishedArgs.TaskType = reply.TaskType
		finishedArgs.TaskNum = reply.TaskNum
		call("Coordinator.TaskFinished", &finishedArgs, &FinishedTaskReply{})

	}
}

func intermediateName(x string, y string) string {
	return "mr-" + x + "-" + y + ".json"
}

func executeMap(fileName string, taskNum int, NReduce int, mapf func(string, string) []KeyValue) {
	// 对其进行map相关操作
	intermediate := []KeyValue{}
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
	intermediate = append(intermediate, kva...)
	tempFiles := []*os.File{}
	tempFileNames := []string{}
	tempEncoders := []*json.Encoder{}
	// 先创建临时文件
	for i := 0; i < NReduce; i++ {
		tempFile, _ := ioutil.TempFile("", "")
		tempFiles = append(tempFiles, tempFile)
		name := tempFile.Name()
		tempFileNames = append(tempFileNames, name)
		encoder := json.NewEncoder(tempFile)
		tempEncoders = append(tempEncoders, encoder)
	}
	// 对每个文件内容进行填充
	for _, kv := range intermediate {
		Y := ihash(kv.Key) % NReduce // 根绝key的hash值取余，来写入相应的文件夹
		err = tempEncoders[Y].Encode(kv)
		if err != nil {
			log.Fatal("add kv error")
		}
	}
	//关闭所有文件
	for _, f := range tempFiles {
		f.Close()
	}

	for i := 0; i < NReduce; i++ {
		createFileWithAutomic(taskNum, i, tempFileNames[i])
	}

}

func createFileWithAutomic(taskNum int, i int, tempFileName string) {
	filename := getIntermediateFileName(taskNum, i)
	os.Rename(tempFileName, filename)
}

func getIntermediateFileName(x int, y int) string {
	return fmt.Sprintf("mr-%d-%d", x, y)
}

func executeReduce(taskNum int, NMapTasks int, reducef func(string, []string) string) {
	// 打开临时文件
	var kva []KeyValue

	for i := 0; i < NMapTasks; i++ {
		//log.Print(getIntermediateFileName(i, taskNum))
		file, err := os.Open(getIntermediateFileName(i, taskNum))
		if err != nil{
			log.Fatal("open the intermediateFile error")
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))

	file, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal("create reducefile fail")
	}
	filename := file.Name()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}

	createFinalFileNameWithAutomic(filename, taskNum)
	file.Close()
}

func createFinalFileNameWithAutomic(s string, i int) {
	filename := fmt.Sprintf("mr-out-%d", i)
	os.Rename(s, filename)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
