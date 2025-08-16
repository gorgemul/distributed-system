package mr

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type RpcPlaceholder struct {
}

type KeepAliveArgs struct {
	WorkerId int
}

type GetMapperJobArgs struct {
	WorkerId int
}

type GetMapperJobReply struct {
	File    string
	NReduce int
	Quit    bool
}

type MapperEmitArgs struct {
	WorkerId     int
	ReducerIndex int
	Key          string
	Values       []string
}

type PutMapperJobArgs struct {
	WorkerId int
	File     string
	Success  bool
}

type GetReducerJobArgs struct {
	WorkerId int
}

type GetReducerJobReply struct {
	ReducerIndex int
}

type PutReducerJobArgs struct {
	WorkerId     int
	ReducerIndex int
	Success      bool
}

func (r GetMapperJobReply) wait() bool {
	if r.File == "" {
		time.Sleep(1 * time.Second)
		return true
	}
	return false
}

func (r GetMapperJobReply) done() bool {
	return r.Quit
}

func (r GetMapperJobReply) run(mapf func(string, string) []KeyValue) {
	data, err := os.ReadFile(r.File)
	if err != nil {
		panic(err)
	}
	intermediate := mapf(r.File, string(data))
	slices.SortFunc(intermediate, func(a, b KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})
	timeout := time.NewTimer(10 * time.Second)
	for i := 0; i < len(intermediate); {
		select {
		case <-timeout.C:
			putMapperJob(PutMapperJobArgs{WorkerId: WorkerId, File: r.File, Success: false})
			return
		default:
			key, j := intermediate[i].Key, i
			values := []string{}
			for j < len(intermediate) && intermediate[j].Key == key {
				values = append(values, intermediate[j].Value)
				j++
			}
			mapperEmit(MapperEmitArgs{WorkerId, ihash(key) % r.NReduce, key, values})
			i = j
		}
	}
	putMapperJob(PutMapperJobArgs{WorkerId: WorkerId, File: r.File, Success: true})
}

func (r GetReducerJobReply) wait() bool {
	if r.ReducerIndex == -1 {
		time.Sleep(1 * time.Second)
		return true
	}
	return false
}

func (r GetReducerJobReply) run(reducef func(string, []string) string) {
	output, err := os.Create(fmt.Sprintf("mr-out-%d", r.ReducerIndex))
	if err != nil {
		panic(err)
	}
	defer output.Close()
	data, err := os.ReadFile(fmt.Sprintf("mr-intermediate-%d.json", r.ReducerIndex))
	if err != nil {
		panic(err)
	}
	var m map[string][]string
	if err := json.Unmarshal(data, &m); err != nil {
		panic(err)
	}
	sortedKeys := slices.Sorted(maps.Keys(m))
	timeout := time.NewTimer(10 * time.Second)
	for _, key := range sortedKeys {
		select {
		case <-timeout.C:
			putReducerJob(PutReducerJobArgs{WorkerId: WorkerId, ReducerIndex: r.ReducerIndex, Success: false})
			return
		default:
			fmt.Fprintf(output, "%v %v\n", key, reducef(key, m[key]))
		}
	}
	putReducerJob(PutReducerJobArgs{WorkerId: WorkerId, ReducerIndex: r.ReducerIndex, Success: true})
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
