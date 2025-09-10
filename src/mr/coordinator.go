package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	M                   int
	R                   int
	inputFiles          []string
	intermediateFiles   [][]string
	mapTasksAssigned    int
	reduceTasksAssigned int
	mapTasksDone        int
	reduceTasksDone     int
	mapWorkers          []string
	reduceWorkers       []string
	workersKilled       int
	mu                  sync.Mutex
}

//Todo(sambhav): Change to gRPC
// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) UpdateStatus(args *StatusUpdate, reply *StatusUpdateReply) error {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == "map" {
		c.mapTasksDone++
	}
	if args.TaskType == "reduce" {
		c.reduceTasksDone++
	}
	return nil
}
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if !args.TaskRequired {
		return nil
	}

	if c.Done() {
		reply.TaskType = "done"
		c.workersKilled++
		return nil
	}

	if c.mapTasksAssigned < c.M {
		reply.TaskType = "map"
		reply.TaskNumber = c.mapTasksAssigned
		// fmt.Println("map task number", c.mapTasksAssigned, "size of input files", len(c.inputFiles))
		reply.InputFile = &c.inputFiles[c.mapTasksAssigned]
		reply.NumFiles = c.R
		c.mapWorkers[c.mapTasksAssigned] = args.WorkerId
		c.mapTasksAssigned++
		return nil
	}
	if c.mapTasksDone == c.M && c.reduceTasksAssigned < c.R {
		reply.TaskType = "reduce"
		reply.TaskNumber = c.reduceTasksAssigned
		reply.InputFile = nil
		reply.NumFiles = c.M
		c.reduceWorkers[c.reduceTasksAssigned] = args.WorkerId
		c.reduceTasksAssigned++
		return nil
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Todo(sambhav): Learn this for interviews
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.mapTasksDone == c.M && c.reduceTasksDone == c.R
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{M: len(files), R: nReduce, mapTasksAssigned: 0, reduceTasksAssigned: 0, inputFiles: files,
		workersKilled: 0, mapTasksDone: 0, reduceTasksDone: 0,
		mapWorkers: make([]string, len(files)), reduceWorkers: make([]string, nReduce)}

	// Initialize 2D slice for intermediate files [M][R]
	c.intermediateFiles = make([][]string, c.M)
	for i := range c.intermediateFiles {
		c.intermediateFiles[i] = make([]string, c.R)
	}
	for _, file := range c.inputFiles {
		fmt.Println(file)

	}

	// Your code here.

	c.server()
	return &c
}
