package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	M                 int
	R                 int
	inputFiles        []string
	intermediateFiles [][]string
	outputFiles       []string
	mapTasksDone      int
	reduceTasksDone   int
	mapWorkers        []string
	reduceWorkers     []string
	workersKilled     int
}

//Todo(sambhav): Change to gRPC
// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// Your code here.
	if !args.TaskRequired {
		return nil
	}

	if c.Done() {
		reply.TaskType = "done"
		c.workersKilled++
		return nil
	}

	if c.mapTasksDone < c.M {
		reply.TaskType = "map"
		reply.TaskNumber = c.mapTasksDone
		reply.inputFile = &c.inputFiles[c.mapTasksDone]
		c.mapWorkers[c.mapTasksDone] = args.WorkerId
		c.mapTasksDone++
		return nil
	}
	if c.reduceTasksDone < c.R {
		reply.TaskType = "reduce"
		reply.TaskNumber = c.reduceTasksDone
		reply.inputFile = nil
		c.reduceWorkers[c.reduceTasksDone] = args.WorkerId
		c.reduceTasksDone++
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
	c := Coordinator{M: len(files), R: nReduce, mapTasksDone: 0, reduceTasksDone: 0, inputFiles: files,
		outputFiles: make([]string, nReduce), workersKilled: 0,
		mapWorkers: make([]string, 0), reduceWorkers: make([]string, 0)}

	// Initialize 2D slice for intermediate files [M][R]
	c.intermediateFiles = make([][]string, c.M)
	for i := range c.intermediateFiles {
		c.intermediateFiles[i] = make([]string, c.R)
	}

	// Your code here.

	c.server()
	return &c
}
