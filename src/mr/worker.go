package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Workers can ask for tasks periodically and sleep in between.
// Workers write to temporary files then rename the result file atmoically.

// Mapper notes
// Each mapper needs to create nReduce intermediate files for reduce tasks.
// Mapper should put intermediate files in the current directory.
// Name the intermediate files mr-X-Y, where X is the Map task number and Y is
// the Reduce task number.
// Use encoding/json package to write key/value pairs to an intermediate file.

// Reducer notes
// The output of the nth reducer is in file mr-out-n.

// How to exit after the whole job finished: worker calls call() and if it fails,
// exit.

// TODO: worker writes to a temporary file then rename atomically.

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	var fileMapped string
	var reduceDone int
	for {
		req := GetTaskReq{FileDone: fileMapped, ReduceDone: reduceDone}
		resp := GetTaskResp{}
		err := call("Coordinator.GetTask", &req, &resp)
		if err != nil {
			fmt.Printf("GetTask got error: %v", err)
			break
		}
		// No file to map.
		if resp.Filename != "" {
			mapFile(resp.Filename, mapf, resp.NReduce)
			fileMapped = resp.Filename
		} else if resp.ReduceNum != 0 {
			reduce(resp.ReduceNum, reducef, resp.NReduce)
			reduceDone = resp.ReduceNum
		} else {
			fmt.Println("sleep coz no task from coordinator")
			fileMapped = ""
			reduceDone = 0
			time.Sleep(1 * time.Second)
		}
	}
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
	// if err == nil {
	// 	return true
	// }

	// fmt.Println(err)
	// return false
}


func mapFile(filename string, mapf func(string, string) []KeyValue, nReduce int) {
	oPrefix := "mr-" + path.Base(filename) + "-"
	encoders := []*json.Encoder{nil}
	for i := 1; i <= nReduce; i++ {
		oname := oPrefix + strconv.Itoa(i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("create %s got error: %v", oname, err)
		}
		defer ofile.Close()
		// ofiles[i] corresponds to mr-filename-i
		encoders = append(encoders, json.NewEncoder(ofile))
	}
	fmt.Printf("opening file: %s\n", filename)
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %s", filename)
	}
	defer f.Close()
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %s", filename)
	}
	// fmt.Printf("file %s content %s\n", filename, string(content[:200]))

	res := mapf(filename, string(content))
	// enc := json.NewEncoder(ofile)
	for _, p := range res {
		k := ihash(p.Key) % nReduce
		err = encoders[k + 1].Encode(&p)
		if err != nil {
			log.Fatalf("cannot encode: %v", p)
		}
	}
	// for _, p := range res {
	// 	fmt.Fprintf(ofile, "%v %v\n", p.Key, p.Value)
	// }
}

func reduce(reduceNum int, reducef func(string, []string) string, nReduce int) {
	inames, err := filepath.Glob("*-" + strconv.Itoa(reduceNum))
	if err != nil {
		log.Fatalf("matching intermediate files got error: %v", inames)
	}
	log.Printf("match files %v\n", inames)

	var ipairs []KeyValue
	for _, i := range inames {
		file, err := os.Open(i)
		if err != nil {
			log.Fatalf("open file %s got error: %v", i, err)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			ipairs = append(ipairs, kv)
		}
	}
	sort.Sort(ByKey(ipairs))

	oname := "mr-out-" + strconv.Itoa(reduceNum)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(ipairs) {
		j := i + 1
		for j < len(ipairs) && ipairs[j].Key == ipairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, ipairs[k].Value)
		}
		output := reducef(ipairs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", ipairs[i].Key, output)

		i = j
	}
	ofile.Close()
}
