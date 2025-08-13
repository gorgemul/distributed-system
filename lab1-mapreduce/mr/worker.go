package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"time"
	"os"
	"slices"
	"strings"
	"encoding/json"
	"maps"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		reply := getMapperJob()
		if reply.Quit {
			break
		}
		if reply.File == "" {
			time.Sleep(1 * time.Second)
			continue
		}
		timeout := time.NewTimer(10 * time.Second)
		go func() {
			<-timeout.C
			putMapperJob(PutMapperJobArgs{File: reply.File, Success: false})
		}()
		data, err := os.ReadFile(reply.File)
		if err != nil {
			panic(err)
		}
		intermediate := mapf(reply.File, string(data))
		slices.SortFunc(intermediate, func(a, b KeyValue) int {
			return strings.Compare(a.Key, b.Key)
		})
		i := 0
		for i < len(intermediate) {
			k := i + 1
			for k < len(intermediate) && intermediate[k].Key == intermediate[i].Key {
				k++
			}
			values := []string{}
			for j := i; j < k; j++ {
				values = append(values, intermediate[j].Value)
			}
			mapperEmit(MapperEmitArgs{ihash(intermediate[i].Key) % reply.NReduce, intermediate[i].Key, values})
			i = k
		}
		timeout.Stop()
		putMapperJob(PutMapperJobArgs{File: reply.File, Success: true})
	}
	for {
		reply, ok := getReducerJob()
		if !ok {
			break
		}
		if reply.ReducerIndex == -1 {
			time.Sleep(1 * time.Second)
			continue
		}
		timeout := time.NewTimer(10 * time.Second)
		go func() {
			<-timeout.C
			putReducerJob(PutReducerJobArgs{ReducerIndex: reply.ReducerIndex, Success: false})
		}()
		data, err := os.ReadFile(fmt.Sprintf("mr-intermediate-%d.json", reply.ReducerIndex))
		if err != nil {
			panic(err)
		}
		var m map[string][]string
		if err := json.Unmarshal(data, &m); err != nil {
			panic(err)
		}
		output, err := os.Create(fmt.Sprintf("mr-out-%d", reply.ReducerIndex))
		if err != nil {
			panic(err)
		}
		sortedKeys := slices.Sorted(maps.Keys(m))
		for _, key := range sortedKeys {
			fmt.Fprintf(output, "%v %v\n", key, reducef(key, m[key]))
		}
		timeout.Stop()
		output.Close()
		putReducerJob(PutReducerJobArgs{ReducerIndex: reply.ReducerIndex, Success: true})
	}
}

func getMapperJob() GetMapperJobReply {
	args := RpcPlaceholder{}
	reply := GetMapperJobReply{}
	if ok := call("Coordinator.GetMapperJob", &args, &reply); !ok {
		log.Fatal("getMapperJob call fail")
	}
	return reply
}

func mapperEmit(args MapperEmitArgs) {
	reply := RpcPlaceholder{}
	if ok := call("Coordinator.MapperEmit", &args, &reply); !ok {
		log.Fatal("mapperEmit call fail")
	}
}

func putMapperJob(args PutMapperJobArgs) {
	reply := RpcPlaceholder{}
	if ok := call("Coordinator.PutMapperJob", &args, &reply); !ok {
		log.Fatal("putMapperJob call fail")
	}
}

func getReducerJob() (reply GetReducerJobReply, ok bool) {
	args := RpcPlaceholder{}
	reply = GetReducerJobReply{}
	if ok = call("Coordinator.GetReducerJob", &args, &reply); !ok { // not ok means that coordinator has quit, indicating that all reducer jobs has finished, so reducer can quit
		return
	}
	return
}

func putReducerJob(args PutReducerJobArgs) {
	reply := RpcPlaceholder{}
	if ok := call("Coordinator.PutReducerJob", &args, &reply); !ok {
		log.Fatal("putMapperJob call fail") // fatal the error since can't fail
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
