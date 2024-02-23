package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	WAITTING int = 0
	STARTED  int = 1
	FINISHED int = 2
)

type Coordinator struct {
	// Your definitions here.
	Mutex          sync.Mutex
	Cond           *sync.Cond
	mapTaskDone    bool
	reduceTaskDone bool
	mapTasks       []*MapTask
	reduceTasks    []*ReduceTask
}

type MapTask struct {
	Id       int
	FileName string
	NReduce  int
	State    int
}

type ReduceTask struct {
	Id    int
	Nmap  int
	State int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply * FetchTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for {
		if c.MapTaskDone() {
			break
		} else if task := c.fetchMapTask(); task != nil {
			reply.MapTask = task
			c.mapTaskStarted(task)
			return nil
		} else {
			c.Cond.Wait()
		}
	}

	for {
		if c.Done() {
			reply.Done = true
			break
		} else if task := c.fetchReduceTask(); task != nil {
			reply.ReduceTask = task
			c.reduceTaskStarted(task)
			return nil
		} else {
			c.Cond.Wait()
		}
	}
	
	return nil
}

func (c *Coordinator) fetchMapTask() *MapTask {
	for _, task := range c.mapTasks {
		if task.State == WAITTING {
			return task
		}
	}
	return nil
}

func (c *Coordinator) mapTaskStarted(task *MapTask) {
	task.State = STARTED

	go func (task *MapTask)  {
		timedue := time.After(10 * time.Second)
		<- timedue
		c.Mutex.Lock()
		defer c.Mutex.Unlock()
		if task.State != FINISHED {
			log.Printf("recover map task %d \n", task.Id)
			task.State = WAITTING
			c.Cond.Broadcast()
		}
	} (task)
}

func (c *Coordinator) MapTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.mapTasks[args.TaskId].State = FINISHED
	if c.MapTaskDone() {
		c.Cond.Broadcast()
	}
	return nil
}

func (c *Coordinator) MapTaskDone() bool {
	if c.mapTaskDone {
		return true
	}
	for _, task := range c.mapTasks {
		if task.State != FINISHED {
			return false
		}
	}
	c.mapTaskDone = true
	return true
}

func (c *Coordinator) fetchReduceTask() *ReduceTask {
	for _, task := range c.reduceTasks {
		if task.State == WAITTING {
			return task
		}
	}
	return nil
}

func (c *Coordinator) reduceTaskStarted(task *ReduceTask) {
	task.State = STARTED
	// recovery function
	go func(task *ReduceTask) {
		timedue := time.After(10 * time.Second)
		<-timedue
		c.Mutex.Lock()
		defer c.Mutex.Unlock()
		if task.State != FINISHED {
			log.Printf("recover reduce task %d \n", task.Id)
			task.State = WAITTING
			c.Cond.Broadcast()
		}
	}(task)
}

func (c *Coordinator) ReduceTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.reduceTasks[args.TaskId].State = FINISHED
	if c.Done() {
		c.Cond.Broadcast()
	}
	return nil
}



// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false
	// return ret
	
	// Your code here.
	if c.reduceTaskDone {
		return true
	}
	for _, task := range c.reduceTasks {
		if task.State != FINISHED {
			return false
		}
	}
	c.reduceTaskDone = true
	return true
	
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Cond = sync.NewCond(&c.Mutex)
	// c.reduceTasksCond = sync.NewCond(&c.reduceTasksMutex)
	c.mapTasks = make([]*MapTask, 0)
	for i, filename := range files {
		task := &MapTask{
			Id:       i,
			FileName: filename,
			NReduce:  nReduce,
			State:    WAITTING,
		}
		c.mapTasks = append(c.mapTasks, task)
	}

	c.reduceTasks = make([]*ReduceTask, 0)
	for i := 0; i < nReduce; i++ {
		task := &ReduceTask{
			Id:    i,
			Nmap:  len(files),
			State: WAITTING,
		}
		c.reduceTasks = append(c.reduceTasks, task)
	}
	c.server()
	return &c
}
