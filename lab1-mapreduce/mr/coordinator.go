package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type jobStatus int
type workerType int
type workerStatus struct {
	kind          workerType
	mapperJobKey  string
	reducerJobKey int
	pingTime      time.Time
}

type Coordinator struct {
	nReduce                 int
	quitLock                sync.Mutex
	quit                    bool
	workerIntermediateLock  sync.Mutex
	workerIntermediate      map[int][]map[string][]string
	intermediateLock        sync.Mutex
	intermediate            [][]map[string][]string
	jobLock                 sync.Mutex
	mapperJobs              map[string]jobStatus
	mapperJobsSuccessCount  int
	reducerJobs             map[int]jobStatus
	reducerJobsSuccessCount int
	checkerLock             sync.Mutex
	checker                 map[int]workerStatus
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
			c.checkerLock.Lock()
			c.checker[args.WorkerId] = workerStatus{kind: Mapper, mapperJobKey: file, pingTime: time.Now()}
			c.checkerLock.Unlock()
			c.mapperJobs[file] = Exec
			reply.File = file
			reply.NReduce = c.nReduce
			break
		}
	}
	return nil
}

func (c *Coordinator) MapperEmit(args *MapperEmitArgs, _ *RpcPlaceholder) error {
	c.workerIntermediateLock.Lock()
	defer c.workerIntermediateLock.Unlock()
	intermediate, ok := c.workerIntermediate[args.WorkerId]
	if !ok {
		c.workerIntermediate[args.WorkerId] = make([]map[string][]string, c.nReduce)
		intermediate = c.workerIntermediate[args.WorkerId]
		for i := range c.nReduce {
			intermediate[i] = make(map[string][]string)
		}
	}
	intermediate[args.ReducerIndex][args.Key] = append(intermediate[args.ReducerIndex][args.Key], args.Values...)
	return nil
}

func (c *Coordinator) PutMapperJob(args *PutMapperJobArgs, _ *RpcPlaceholder) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	if args.Success {
		c.mapperJobs[args.File] = Success
		c.mapperJobsSuccessCount++
		c.intermediateLock.Lock()
		c.workerIntermediateLock.Lock()
		c.intermediate = append(c.intermediate, c.workerIntermediate[args.WorkerId])
		delete(c.workerIntermediate, args.WorkerId)
		c.workerIntermediateLock.Unlock()
		if c.mapperJobsSuccessCount == len(c.mapperJobs) {
			intermediate := make([]map[string][]string, c.nReduce)
			for i := range c.nReduce {
				intermediate[i] = make(map[string][]string)
			}
			for _, item := range c.intermediate {
				for i, m := range item {
					for k, vs := range m {
						intermediate[i][k] = append(intermediate[i][k], vs...)
					}
				}
			}
			for i := range c.nReduce {
				data, err := json.Marshal(intermediate[i])
				if err != nil {
					panic(err)
				}
				if err = os.WriteFile(fmt.Sprintf("mr-intermediate-%d.json", i), data, 0644); err != nil {
					panic(err)
				}
			}
			c.intermediate = nil
		}
		c.intermediateLock.Unlock()
	} else {
		c.mapperJobs[args.File] = Ready
		c.workerIntermediateLock.Lock()
		delete(c.workerIntermediate, args.WorkerId)
		c.workerIntermediateLock.Unlock()
	}
	c.checkerLock.Lock()
	delete(c.checker, args.WorkerId)
	c.checkerLock.Unlock()
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
			c.checkerLock.Lock()
			c.checker[args.WorkerId] = workerStatus{kind: Reducer, reducerJobKey: i, pingTime: time.Now()}
			c.checkerLock.Unlock()
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
	c.checkerLock.Lock()
	delete(c.checker, args.WorkerId)
	c.checkerLock.Unlock()
	return nil
}

func (c *Coordinator) KeepAlive(args *KeepAliveArgs, _ *RpcPlaceholder) error {
	c.checkerLock.Lock()
	defer c.checkerLock.Unlock()
	worker, ok := c.checker[args.WorkerId]
	// indicating that worker hasn't getting any job yet
	if !ok {
		return nil
	}
	worker.pingTime = time.Now()
	c.checker[args.WorkerId] = worker
	return nil
}

func (c *Coordinator) check() {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			c.checkerLock.Lock()
			for id, status := range c.checker {
				if time.Since(status.pingTime).Seconds() >= 3 {
					c.jobLock.Lock()
					switch status.kind {
					case Mapper:
						c.mapperJobs[status.mapperJobKey] = Ready
					case Reducer:
						c.reducerJobs[status.reducerJobKey] = Ready
					}
					c.jobLock.Unlock()
					c.workerIntermediateLock.Lock()
					delete(c.workerIntermediate, id)
					c.workerIntermediateLock.Unlock()
					delete(c.checker, id)
				}
			}
			c.checkerLock.Unlock()
		}
	}()
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.quitLock.Lock()
	defer c.quitLock.Unlock()
	return c.quit
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		nReduce:            nReduce,
		mapperJobs:         make(map[string]jobStatus),
		reducerJobs:        make(map[int]jobStatus),
		workerIntermediate: make(map[int][]map[string][]string),
		checker:            make(map[int]workerStatus),
	}
	for _, file := range files {
		c.mapperJobs[file] = Ready
	}
	for i := range nReduce {
		c.reducerJobs[i] = Ready
	}
	c.server()
	c.check()
	return c
}
