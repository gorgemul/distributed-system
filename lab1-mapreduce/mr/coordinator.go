package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
import "encoding/json"

type jobStatus int

const (
	Ready jobStatus = iota
	Exec
	Success
)

type Coordinator struct {
	// Your definitions here.
	nReduce                 int
	jobLock                 sync.Mutex
	quit                    bool
	intermediateLocks       []sync.Mutex
	intermediate            []map[string][]string
	mapperJobs              map[string]jobStatus
	mapperJobsSuccessCount  int
	reducerJobs             map[int]jobStatus
	reducerJobsSuccessCount int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetMapperJob(_ *RpcPlaceholder, reply *GetMapperJobReply) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	if c.mapperJobsSuccessCount == len(c.mapperJobs) {
		reply.Quit = true
		return nil
	}
	for file, status := range c.mapperJobs {
		if status == Ready { 
			c.mapperJobs[file] = Exec
			reply.File = file
			reply.NReduce = c.nReduce
			break
		}
	}
	return nil
}

func (c *Coordinator) MapperEmit(args *MapperEmitArgs, _ *RpcPlaceholder) error {
	c.intermediateLocks[args.ReducerIndex].Lock()
	defer c.intermediateLocks[args.ReducerIndex].Unlock()
	c.intermediate[args.ReducerIndex][args.Key] = append(c.intermediate[args.ReducerIndex][args.Key], args.Values...)
	return nil
}

func (c *Coordinator) PutMapperJob(args *PutMapperJobArgs, _ *RpcPlaceholder) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	if args.Success {
		c.mapperJobs[args.File] = Success
		c.mapperJobsSuccessCount++
		if c.mapperJobsSuccessCount == len(c.mapperJobs) {
			for i := range c.nReduce {
				f, err := os.Create(fmt.Sprintf("mr-intermediate-%d.json", i))
				if err != nil {
					panic(err)
				}
				data, err := json.Marshal(c.intermediate[i])
				if err != nil {
					panic(err)
				}
				f.Write(data)
				f.Close()
			}
		}
	} else {
		c.mapperJobs[args.File] = Ready
	}
	return nil
}

func (c *Coordinator) GetReducerJob(_ *RpcPlaceholder, reply *GetReducerJobReply) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	reply.ReducerIndex = -1 // to indicate that no reducer job but not quit yet, since zero value of int is 0, can't distinguish index 0 and zero value
	if c.reducerJobsSuccessCount == len(c.reducerJobs) {
		return nil
	}
	for i, status := range c.reducerJobs {
		if status == Ready {
			c.reducerJobs[i] = Exec
			reply.ReducerIndex = i
			break
		}
	}
	return nil
}

func (c *Coordinator) PutReducerJob(args *PutReducerJobArgs, _ *RpcPlaceholder) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	if args.Success {
		c.reducerJobs[args.ReducerIndex] = Success
		c.reducerJobsSuccessCount++
		if c.reducerJobsSuccessCount == len(c.reducerJobs) {
			c.quit = true
		}
	} else {
		c.reducerJobs[args.ReducerIndex] = Ready
	}
	return nil
}

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
	return c.quit
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	mapperJobs := make(map[string]jobStatus)
	reducerJobs := make(map[int]jobStatus)
	intermediate := make([]map[string][]string, nReduce)
	for _, file := range files {
		mapperJobs[file] = Ready
	}
	for i := range nReduce {
		reducerJobs[i] = Ready
	}
	for i := range intermediate {
		intermediate[i] = make(map[string][]string)
	}
	c.nReduce = nReduce
	c.mapperJobs = mapperJobs
	c.reducerJobs = reducerJobs
	c.intermediateLocks = make([]sync.Mutex, nReduce)
	c.intermediate = intermediate
	c.server()
	return &c
}
