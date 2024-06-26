package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		if reply, ok := callFetchTask(); ok {
			if reply.Done {
				return
			}
			if reply.MapTask != nil {
				doMapTask(reply.MapTask, mapf)
			}
			if reply.ReduceTask != nil {
				doReduceTask(reply.ReduceTask, reducef)
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func doMapTask(task *MapTask, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Printf("open file: %v error", task.FileName)
	}

	content, err := io.ReadAll(file)

	if err != nil {
		log.Printf("read file: %v error", task.FileName)
	}
	file.Close()
	kva := mapf(task.FileName, string(content))
	N2map := make([][]KeyValue, task.NReduce)

	for _, kv := range kva {
		i := ihash(kv.Key) % task.NReduce
		N2map[i] = append(N2map[i], kv)
	}

	for i, ikva := range N2map {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", task.Id, i)
		tmpFile, err := os.CreateTemp("./", "mr-*.tmp")
		if err != nil {
			log.Fatal(err)
		}

		encoder := json.NewEncoder(tmpFile)
		for _, kv := range ikva {
			err := encoder.Encode(kv)

			if err != nil {
				log.Fatalf("json.encode %v error", kv)
			}
		}
		tmpFile.Close()

		if err := os.Rename(tmpFile.Name(), intermediateFilename); err != nil {
			log.Fatal(err)
		}
	}

	callMapTaskFinished(task.Id)
}

func doReduceTask(task *ReduceTask, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < task.Nmap; i++ {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", i, task.Id)

		file, err := os.Open(intermediateFilename)

		if err != nil {
			log.Fatalf("open file: %v error", intermediateFilename)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	
	ofile, err := os.CreateTemp("./", "mr-*.tmp")
	if err != nil {
		log.Fatal(err)
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

	ofile.Close()

	if err := os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d.r", task.Id)); err != nil {
		log.Fatal(err)
	}
	callReduceTaskFinished(task.Id)
}

func callFetchTask() (*FetchTaskReply, bool) {
	args := FetchTaskArgs{}
	reply := FetchTaskReply{}
	ok := call("Coordinator.FetchTask", &args, &reply)
	return &reply, ok
}

func callMapTaskFinished(taskId int) (*TaskFinishedReply, bool) {
	args := TaskFinishedArgs{taskId}
	reply := TaskFinishedReply{}
	ok := call("Coordinator.MapTaskFinished", &args, &reply)
	return &reply, ok
}

func callReduceTaskFinished(taskId int) (*TaskFinishedReply, bool) {
	args := TaskFinishedArgs{taskId}
	reply := TaskFinishedReply{}
	ok := call("Coordinator.ReduceTaskFinished", &args, &reply)
	return &reply, ok
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

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
