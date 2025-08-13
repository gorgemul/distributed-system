package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
)

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for r := getMapperJob(); !r.done(); r = getMapperJob() {
		if !r.wait() {
			r.run(mapf)
		}
	}
	for r, ok := getReducerJob(); ok; r, ok = getReducerJob() {
		if !r.wait() {
			r.run(reducef)
		}
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
	// not ok means that coordinator has quit, indicating that all reducer jobs has finished, so reducer can quit
	if ok = call("Coordinator.GetReducerJob", &args, &reply); !ok {
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
