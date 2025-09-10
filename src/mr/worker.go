package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var workerId string = uuid.New().String()

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func handleMapTask(reply *TaskReply, mapf func(string, string) []KeyValue) {
	// fmt.Printf("####MAP#### worker %s is handling map task %d\n", workerId, reply.TaskNumber)
	// Read input file and create intermediate eky value pairs
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	if reply.InputFile == nil {
		log.Fatalf("input file is nil")
	}
	file, err := os.Open(*reply.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", *reply.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", *reply.InputFile)
	}
	file.Close()

	intermediate := mapf(*reply.InputFile, string(content))

	// Sort intermediate key-value pairs by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// Create file handles for all intermediate files
	files := make([]*os.File, reply.NumFiles)
	for i := 0; i < reply.NumFiles; i++ {
		oname := "mr-" + strconv.Itoa(reply.TaskNumber) + "-" + strconv.Itoa(i)
		file, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		files[i] = file
		defer file.Close()
	}

	for _, kv := range intermediate {
		key := kv.Key
		value := kv.Value
		bucket := ihash(key) % reply.NumFiles

		// Write key-value pair to the appropriate intermediate file
		fmt.Fprintf(files[bucket], "%v %v\n", key, value)
	}
	// fmt.Printf("####MAP#### worker %s is done handling map task %d\n", workerId, reply.TaskNumber)

	// Print actual filenames created
	// fmt.Print("Files created: ")
	// for _, file := range files {
	// 	fmt.Print(file.Name() + " ")
	// }
	// fmt.Println()
}

func handleReduceTask(reply *TaskReply, reducef func(string, []string) string) {
	// fmt.Printf("####REDUCE#### worker %s is handling reduce task %d\n", workerId, reply.TaskNumber)

	intermediate := []KeyValue{}
	for i := 0; i < reply.NumFiles; i++ {
		intermediateFile := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskNumber)
		file, err := os.Open(intermediateFile)

		if err != nil {
			file.Close()
			log.Fatalf("cannot open %v", intermediateFile)
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			file.Close()
			log.Fatalf("cannot read %v", intermediateFile)
		}
		file.Close()

		// Parse the content line by line
		lines := strings.Split(string(content), "\n")
		for lineIndex, line := range lines {
			// fmt.Println(" line is ",line)
			line = strings.TrimSpace(line)
			if line == "" {
				if lineIndex != len(lines)-1 {
					fmt.Println("ANARTH!")
				}
				continue
			}
			// Split each line into key and value
			parts := strings.SplitN(line, " ", 2)
			if len(parts) == 2 {
				kv := KeyValue{Key: parts[0], Value: parts[1]}
				intermediate = append(intermediate, kv)
			} else {
				log.Fatalf("ANARTH! Why does a line contain !=2 values?: %v", line)
			}
		}
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	ofileName := "mr-out-" + strconv.Itoa(reply.TaskNumber)
	ofile, err := os.Create(ofileName)
	if err != nil {
		log.Fatalf("cannot create %v", ofileName)
	}
	defer ofile.Close()
	for i := 0; i < len(intermediate); {
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
	// fmt.Printf("####REDUCE#### worker %s is done handling reduce task %d\n", workerId, reply.TaskNumber)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Your worker implementation here.

		reply := &TaskReply{}
		args := &TaskArgs{
			TaskRequired: true,
			WorkerId:     workerId,
		}

		ok := call("Coordinator.GetTask", args, reply)
		if !ok {
			fmt.Printf("call failed!")
			return
		}

		if reply.TaskType == "done" {
			// fmt.Printf("worker %s is done\n", workerId)
			os.Exit(0)
		}

		switch reply.TaskType {
		case "map":
			handleMapTask(reply, mapf)
			updateArgs := &StatusUpdate{
				TaskType: "map",
			}
			updateReply := &StatusUpdateReply{}
			ok := call("Coordinator.UpdateStatus", updateArgs, updateReply)
			if !ok {
				fmt.Printf("update call failed!")
				return
			}
		case "reduce":
			handleReduceTask(reply, reducef)
			updateArgs := &StatusUpdate{
				TaskType: "reduce",
			}
			updateReply := &StatusUpdateReply{}
			ok := call("Coordinator.UpdateStatus", updateArgs, updateReply)
			if !ok {
				fmt.Printf("update call failed!")
				return
			}
		}

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()

		// Ask for a task from the coordinator
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
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
