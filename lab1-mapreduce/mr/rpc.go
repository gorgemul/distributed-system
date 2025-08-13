package mr

import (
	"os"
	"strconv"
	"time"
	"slices"
	"strings"
	"fmt"
	"encoding/json"
	"maps"
)

type RpcPlaceholder struct {
}

type GetMapperJobReply struct {
	File    string
	NReduce int
	Quit    bool
}

type MapperEmitArgs struct {
	ReducerIndex int
	Key          string
	Values       []string
}

type PutMapperJobArgs struct {
	File    string
	Success bool
}

type GetReducerJobReply struct {
	ReducerIndex int
}

type PutReducerJobArgs struct {
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
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	go func() {
		<-timeout.C
		putMapperJob(PutMapperJobArgs{File: r.File, Success: false})
	}()
	data, err := os.ReadFile(r.File)
	if err != nil {
		panic(err)
	}
	intermediate := mapf(r.File, string(data))
	slices.SortFunc(intermediate, func(a, b KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})
	for i := 0; i < len(intermediate); {
		key, j := intermediate[i].Key, i
		values := []string{}
		for j < len(intermediate) && intermediate[j].Key == key {
			values = append(values, intermediate[j].Value)
			j++
		}
		mapperEmit(MapperEmitArgs{ihash(key) % r.NReduce, key, values})
		i = j
	}
	putMapperJob(PutMapperJobArgs{File: r.File, Success: true})
}

func (r GetReducerJobReply) wait() bool {
	if r.ReducerIndex == -1 {
		time.Sleep(1 * time.Second)
		return true
	}
	return false
}

func (r GetReducerJobReply) run(reducef func(string, []string) string) {
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	go func() {
		<-timeout.C
		putReducerJob(PutReducerJobArgs{ReducerIndex: r.ReducerIndex, Success: false})
	}()
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
	for _, key := range sortedKeys {
		fmt.Fprintf(output, "%v %v\n", key, reducef(key, m[key]))
	}
	putReducerJob(PutReducerJobArgs{ReducerIndex: r.ReducerIndex, Success: true})
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
