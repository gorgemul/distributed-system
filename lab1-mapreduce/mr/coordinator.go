package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"fmt"
	"encoding/json"
	"time"
)

type jobStatus int
type workerType int
type workerStatus struct {
	kind workerType
	mapperJobKey string
	reducerJobKey int
	pingTime time.Time
}
type Coordinator struct {
	// Your definitions here.
	nReduce                 int

	quit                    bool
	quitLock                sync.Mutex

	intermediateLocks       []sync.Mutex
	intermediate            []map[string][]string

	jobLock                 sync.Mutex
	mapperJobs              map[string]jobStatus
	mapperJobsSuccessCount  int
	reducerJobs             map[int]jobStatus
	reducerJobsSuccessCount int

	workerTrackerLock       sync.Mutex
	workerTracker           map[int]workerStatus
}

const (
	Ready jobStatus = iota
	Exec
	Success
)

const (
	Mapper workerType = iota
	Reducer
)

func (c *Coordinator) GetMapperJob(args *GetMapperJobArgs, reply *GetMapperJobReply) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	if c.mapperJobsSuccessCount == len(c.mapperJobs) {
		reply.Quit = true
		return nil
	}
	for file, status := range c.mapperJobs {
		if status == Ready { 
			c.workerTrackerLock.Lock()
			c.workerTracker[args.WorkerId] = workerStatus{kind: Mapper, mapperJobKey: file, pingTime: time.Now()}
			c.workerTrackerLock.Unlock()
			c.mapperJobs[file] = Exec
			reply.File = file
			reply.NReduce = c.nReduce
			break
		}
	}
	return nil
}

// TODO: handle partially emit
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
			//TODO: should have a [WorkerId][ReducerIndex]{k, values}, and aggregate it to c.intermediate[i]
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
	c.workerTrackerLock.Lock()
	// log.Printf("[SERVER]: mapper remove worker %d from tracker", args.WorkerId)
	delete(c.workerTracker, args.WorkerId)
	c.workerTrackerLock.Unlock()
	return nil
}

func (c *Coordinator) GetReducerJob(args *GetReducerJobArgs, reply *GetReducerJobReply) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	reply.ReducerIndex = -1 // to indicate that no reducer job but not quit yet, since zero value of int is 0, can't distinguish index 0 and zero value
	if c.reducerJobsSuccessCount == len(c.reducerJobs) {
		return nil
	}
	for i, status := range c.reducerJobs {
		if status == Ready {
			c.workerTrackerLock.Lock()
			// log.Printf("assign reducer %d to worker %d", i, args.WorkerId)
			c.workerTracker[args.WorkerId] = workerStatus{kind: Reducer, reducerJobKey: i, pingTime: time.Now()}
			c.workerTrackerLock.Unlock()
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
			c.quitLock.Lock()
			c.quit = true
			c.quitLock.Unlock()
		}
	} else {
		c.reducerJobs[args.ReducerIndex] = Ready
	}
	c.workerTrackerLock.Lock()
	// log.Printf("[SERVER]: reducer remove worker %d from tracker", args.WorkerId)
	delete(c.workerTracker, args.WorkerId)
	c.workerTrackerLock.Unlock()
	return nil
}

func (c *Coordinator) KeepAlive(args *KeepAliveArgs, _ *RpcPlaceholder) error {
	c.workerTrackerLock.Lock()
	defer c.workerTrackerLock.Unlock()
	ws, ok := c.workerTracker[args.WorkerId]
	// log.Printf("[SERVER] receive client %d ping", args.WorkerId)
	// indicating that worker hasn't getting any job yet
	if !ok { 
		// log.Printf("[SERVER] client %d not has job yet", args.WorkerId)
		return nil
	}
	ws.pingTime = time.Now()
	c.workerTracker[args.WorkerId] = ws
	return nil
}

func (c *Coordinator) trackWorkers() {
	ticker := time.NewTicker(3 * time.Second)
	go func() {
		for {
			<-ticker.C
			c.workerTrackerLock.Lock()
			for id, status := range c.workerTracker {
				if time.Since(status.pingTime).Seconds() >= 3 {
					c.jobLock.Lock()
					switch status.kind {
					case Mapper:
						c.mapperJobs[status.mapperJobKey] = Ready
					case Reducer:
						c.reducerJobs[status.reducerJobKey] = Ready
					}
					c.jobLock.Unlock()
					// log.Printf("worker %v is being deleted", id)
					delete(c.workerTracker, id)
				}
			}
			c.workerTrackerLock.Unlock()
		}
	}()
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
	c.quitLock.Lock()
	q := c.quit
	c.quitLock.Unlock()
	return q
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
	c.workerTracker = make(map[int]workerStatus)
	c.server()
	c.trackWorkers()
	return &c
}
