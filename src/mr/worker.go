package mr

import "encoding/json"
import "fmt"
import "hash/fnv"
import "log"
import "net/rpc"
import "os"
import "path/filepath"
import "sort"
import "strings"
import "time"

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

var coordSockName string // socket for coordinator

// call() sends an RPC and waits for the response.
// returns false if the server could not be reached.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordSockName
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("cannot dial", rpcname, ":", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	} else {
		log.Println(rpcname, "call failed:", err)
		return false
	}
}

// ask asks the coordinator for the next unit of work, retrying a transient
// dial failure so workers survive a coordinator that is still starting.
func ask(args *AskTaskRequest, reply *AskTaskReply) bool {
	for attempt := 0; attempt < 5; attempt++ {
		ok := call("Coordinator.AskTask", &AskTaskRequest{}, reply)
		if ok {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func succeed(args *FinishTaskRequest) bool {
	reply := FinishTaskReply{}
	return call("Coordinator.FinishTask", args, &reply)
}

// intermediateName is the canonical intermediate filename; atomic renames
// make the final appearance its only appearance.
func intermediateName(mapId, reduceId int) string {
	return fmt.Sprintf("mr-tmp-%d-%d", mapId, reduceId)
}

// emitIntermediates partitions one map task into NReduce files, writing each
// through its own temp file plus rename so a crashed attempt leaves nothing
// behind.
func emitIntermediates(mapId, nReduce int, kvs []KeyValue) error {
	buckets := make([]map[string][]string, nReduce)
	for i := range buckets {
		buckets[i] = map[string][]string{}
	}
	for _, kv := range kvs {
		id := ihash(kv.Key) % nReduce
		buckets[id][kv.Key] = append(buckets[id][kv.Key], kv.Value)
	}
	for redId, bucket := range buckets {
		name := intermediateName(mapId, redId)
		part, err := os.CreateTemp(".", "part-*")
		if err != nil {
			return err
		}
		tmp := part.Name()
		enc := json.NewEncoder(part)
		for _, key := range sortedKeys(bucket) {
			for _, value := range bucket[key] {
				if err := enc.Encode(&KeyValue{Key: key, Value: value}); err != nil {
					part.Close()
					os.Remove(tmp)
					return err
				}
			}
		}
		if err := part.Sync(); err != nil {
			part.Close()
			os.Remove(tmp)
			return err
		}
		if err := part.Close(); err != nil {
			os.Remove(tmp)
			return err
		}
		if err := os.Rename(tmp, name); err != nil {
			os.Remove(tmp)
			return err
		}
	}
	return nil
}

func sortedKeys(bucket map[string][]string) []string {
	keys := make([]string, 0, len(bucket))
	for key := range bucket {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// runMapTask produces the intermediate files for one map task.
func runMapTask(id, nReduce int, input string, mapf func(string, string) []KeyValue) (bool, error) {
	content, err := os.ReadFile(input)
	if err != nil {
		log.Println("read", input, ":", err)
		return false, nil
	}
	if err := emitIntermediates(id, nReduce, mapf(input, string(content))); err != nil {
		log.Println("emit", input, ":", err)
		return false, err
	}
	return true, nil
}

// runReduceTask folds every intermediate file for its reduce id. Intermediates
// appear only after their map task was reported finished, so any missing file
// is a scheduling anomaly worth retrying rather than an exit.
func runReduceTask(id, nMap int, reducef func(string, []string) string) (bool, error) {
	intermediate := map[string][]string{}
	for mapId := 0; mapId < nMap; mapId++ {
		content, err := os.ReadFile(intermediateName(mapId, id))
		if err != nil {
			log.Println("reduce", id, "missing", intermediateName(mapId, id), ":", err)
			return false, nil
		}
		dec := json.NewDecoder(strings.NewReader(string(content)))
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}
	}
	keys := make([]string, 0, len(intermediate))
	for key := range intermediate {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out, err := os.CreateTemp(".", "out-*")
	if err != nil {
		return false, err
	}
	tmp := out.Name()
	for _, key := range keys {
		fmt.Fprintf(out, "%v %v\n", key, reducef(key, intermediate[key]))
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmp)
		return false, err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return false, err
	}
	if err := os.Rename(tmp, fmt.Sprintf("mr-out-%d", id)); err != nil {
		os.Remove(tmp)
		return false, err
	}
	_ = filepath.SkipDir
	return true, nil
}

// Worker asks for map tasks, then reduce tasks, and exits when the
// coordinator says the job is over. Wait replies keep polling.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		reply := AskTaskReply{}
		if !ask(&AskTaskRequest{}, &reply) {
			return
		}
		switch reply.Kind {
		case TaskMap:
			ok, err := runMapTask(reply.Id, reply.NReduce, reply.InputFile, mapf)
			if err != nil {
				return
			}
			if ok {
				if !succeed(&FinishTaskRequest{Kind: TaskMap, Id: reply.Id}) {
					return
				}
			}
		case TaskReduce:
			ok, err := runReduceTask(reply.Id, reply.NMap, reducef)
			if err != nil {
				return
			}
			if ok {
				if !succeed(&FinishTaskRequest{Kind: TaskReduce, Id: reply.Id}) {
					return
				}
			}
		case TaskExit:
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// send the RPC request, wait for the reply.
	reply := ExampleReply{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
	}
}
