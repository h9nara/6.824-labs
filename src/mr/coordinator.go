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

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	mapRemaining int
	reduceRemaining int
	nReduce int
	mapDone map[string]bool
	mapDeadline map[string]time.Time
	reduceDone []bool
	reduceDeadline []time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(req *GetTaskReq, resp *GetTaskResp) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.FileDone != "" {
		if !c.mapDone[req.FileDone] {
			c.mapRemaining--
			c.mapDone[req.FileDone] = true
		}
	} else if req.ReduceDone != 0 {
		if !c.reduceDone[req.ReduceDone] {
			c.reduceRemaining--
			c.reduceDone[req.ReduceDone] = true
		}
	}
	if c.mapRemaining > 0 {
		for f, ddl := range c.mapDeadline {
			if c.mapDeadline[f].IsZero() || (time.Now().After(ddl) && !c.mapDone[f]) {
				c.mapDeadline[f] = time.Now().Add(10 * time.Second)
				resp.Filename = f
				resp.NReduce = c.nReduce
				return nil
			}
		}
		// Map phase not finished.
		return nil
	}
	for i := 1; i <= c.nReduce; i++ {
		if c.reduceDeadline[i].IsZero() || (time.Now().After(c.reduceDeadline[i]) && !c.reduceDone[i]) {
			c.reduceDeadline[i] = time.Now().Add(10 * time.Second)
			resp.ReduceNum = i
			resp.NReduce = c.nReduce
			return nil
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// Your code here.
	// TODO: try remove the locking.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reduceRemaining == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapRemaining: len(files),
		reduceRemaining: nReduce,
		mapDone: make(map[string]bool),
		mapDeadline: make(map[string]time.Time),
		reduceDone: make([]bool, nReduce + 1),
		reduceDeadline: make([]time.Time, nReduce + 1),
		nReduce: nReduce,
	}
	for _, f := range files {
		c.mapDeadline[f] = time.Time{}
	}
	for i := 1; i <= nReduce; i++ {
		c.reduceDeadline[i] = time.Time{}
	}
	// Your code here.

	c.server()
	return &c
}
