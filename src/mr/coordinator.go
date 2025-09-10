package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	M                   int
	R                   int
	inputFiles          []string
	intermediateFiles   [][]string
	mapTasksToAssign    []int
	reduceTasksToAssign []int
	mapTasksDone        int
	reduceTasksDone     int
	mapTaskTime         map[int]time.Time
	mapTaskWorkers      map[int]string
	reduceTaskTime      map[int]time.Time
	reduceTaskWorkers   map[int]string
	workersKilled       int
	mu                  sync.Mutex
}

//Todo(sambhav): Change to gRPC
// Your code here -- RPC handlers for the worker to call.

// removePrefixFromFiles finds all files with the given prefix and renames them to remove the prefix
func (c *Coordinator) removePrefixFromFiles(prefix string) {
	pattern := prefix + "*"
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Printf("Error finding files with prefix %s: %v", prefix, err)
		return
	}

	for _, oldFile := range matches {
		newFile := strings.TrimPrefix(oldFile, prefix)
		if newFile != oldFile { // Only rename if prefix was actually found
			err := os.Rename(oldFile, newFile)
			if err != nil {
				log.Printf("Failed to rename %s to %s: %v", oldFile, newFile, err)
			}
		}
	}
}

func (c *Coordinator) UpdateStatus(args *StatusUpdate, reply *StatusUpdateReply) error {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == "map" {
		c.mapTasksDone++

		// Get the worker ID for this task before deleting
		workerID := c.mapTaskWorkers[args.TaskNumber]

		// Remove worker ID prefix from all files created by this worker
		if workerID != "" {
			c.removePrefixFromFiles(workerID)
		}

		delete(c.mapTaskTime, args.TaskNumber)
		delete(c.mapTaskWorkers, args.TaskNumber)
	}
	if args.TaskType == "reduce" {
		c.reduceTasksDone++

		workerID := c.reduceTaskWorkers[args.TaskNumber]
		if workerID != "" {
			c.removePrefixFromFiles(workerID)
		}
		delete(c.reduceTaskTime, args.TaskNumber)
		delete(c.reduceTaskWorkers, args.TaskNumber)
	}
	return nil
}

func (c *Coordinator) PeriodicCheck() {

	for {
		c.mu.Lock()
		fmt.Println("Periodic check")
		for i, assignedTime := range c.mapTaskTime {
			if time.Since(assignedTime) > 10*time.Second {
				fmt.Printf("### CRASH ### Killing map task worker %s\n", c.mapTaskWorkers[i])
				c.mapTasksToAssign = append(c.mapTasksToAssign, i)
				delete(c.mapTaskTime, i)
				delete(c.mapTaskWorkers, i)
			}
		}
		for i, assignedTime := range c.reduceTaskTime {
			if time.Since(assignedTime) > 10*time.Second {
				fmt.Printf("### CRASH ### Killing reduce task worker %s\n", c.reduceTaskWorkers[i])
				c.reduceTasksToAssign = append(c.reduceTasksToAssign, i)
				delete(c.reduceTaskTime, i)
				delete(c.reduceTaskWorkers, i)
			}
		}
		fmt.Println("Sleeping")
		c.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// Your code here.
	fmt.Println("Get tasks called, MapTasksDone: ", c.mapTasksDone, " ReduceTasksDone: ", c.reduceTasksDone)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !args.TaskRequired {
		fmt.Println("Task not required")
		return nil
	}

	if c.Done() {
		fmt.Println("Job done")
		reply.TaskType = "done"
		c.workersKilled++
		return nil
	}

	if len(c.mapTasksToAssign) > 0 {
		fmt.Println("Map tasks to assign: ", len(c.mapTasksToAssign))
		reply.TaskType = "map"

		lastIndex := len(c.mapTasksToAssign) - 1
		reply.TaskNumber = c.mapTasksToAssign[lastIndex]
		reply.InputFile = &c.inputFiles[c.mapTasksToAssign[lastIndex]]
		reply.NumFiles = c.R

		c.mapTaskTime[reply.TaskNumber] = time.Now()
		c.mapTaskWorkers[reply.TaskNumber] = args.WorkerId
		// c.mapWorkers[c.mapTasksAssigned] = args.WorkerId
		c.mapTasksToAssign = c.mapTasksToAssign[:lastIndex]
		return nil
	}
	if c.mapTasksDone == c.M && len(c.reduceTasksToAssign) > 0 {
		fmt.Println("Reduce tasks to assign: ", len(c.reduceTasksToAssign))
		reply.TaskType = "reduce"
		lastIndex := len(c.reduceTasksToAssign) - 1
		reply.TaskNumber = c.reduceTasksToAssign[lastIndex]
		reply.InputFile = nil
		reply.NumFiles = c.M
		c.reduceTasksToAssign = c.reduceTasksToAssign[:lastIndex]
		c.reduceTaskTime[reply.TaskNumber] = time.Now()
		c.reduceTaskWorkers[reply.TaskNumber] = args.WorkerId
		return nil
	}
	reply.TaskType = "wait"

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
	c := Coordinator{M: len(files), R: nReduce, inputFiles: files,
		workersKilled: 0, mapTasksDone: 0, reduceTasksDone: 0}

	// Initialize tracking maps
	c.mapTaskTime = make(map[int]time.Time)
	c.mapTaskWorkers = make(map[int]string)
	c.reduceTaskTime = make(map[int]time.Time)
	c.reduceTaskWorkers = make(map[int]string)

	// Initialize 2D slice for intermediate files [M][R]
	c.intermediateFiles = make([][]string, c.M)

	c.mapTasksToAssign = make([]int, c.M)
	for i := 0; i < c.M; i++ {
		c.mapTasksToAssign[i] = i
	}
	c.reduceTasksToAssign = make([]int, c.R)
	for i := 0; i < c.R; i++ {
		c.reduceTasksToAssign[i] = i
	}
	for i := range c.intermediateFiles {
		c.intermediateFiles[i] = make([]string, c.R)
	}
	for _, file := range c.inputFiles {
		fmt.Println(file)

	}

	go c.PeriodicCheck()

	// Your code here.

	c.server()
	return &c
}
