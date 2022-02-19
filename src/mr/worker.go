package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	workerID := CallWorkerRegister()
	prefix := fmt.Sprintf("Worker id: %v,", workerID)

	args := ReportArgs{}
	args.IsExited = false
	args.IsFinished = false
	args.WorkID = workerID

	// 重复循环
	for {
		reply := CallWorkerGetTask(workerID)
		if reply.NeedExit {
			log.Fatal(prefix + "Receive NeedExit, worker Exit!")
		}
		args.TaskID = reply.TaskID

		// map 阶段完成，开始reduce 阶段
		if reply.MapPhaseIsFinished {
			log.Printf(prefix+"Reduce Phase, Begin Task %d", reply.TaskID)
			args.Phase = REDUCE_PHASE
			maps := make(map[string][]string)

			for i := 0; i < reply.NMap; i++ {
				fileName := reduceName(i, reply.TaskID)
				file, err := os.Open(fileName)
				if err != nil {
					args.IsExited = true
					args.IsFinished = false
					CallReport(&args)
					log.Fatalf(prefix+"open %v Error!", fileName)
				} else {
					log.Printf(prefix+"open file %s successful!", fileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					if _, ok := maps[kv.Key]; !ok {
						maps[kv.Key] = make([]string, 0, 100)
					}
					maps[kv.Key] = append(maps[kv.Key], kv.Value)
				}
			}

			res := make([]string, 0, 100)

			for k, v := range maps {
				res = append(res, fmt.Sprintf("%v %v\n", k, reducef(k, v)))
			}

			if err := ioutil.WriteFile(mergeName(reply.TaskID), []byte(strings.Join(res, "")), 0600); err != nil {
				args.IsExited = true
				args.IsFinished = false
				CallReport(&args)
			}

			args.IsFinished = true
			args.IsExited = false
			CallReport(&args)
			log.Printf(prefix+"Reduce Phase, Finish Task %d", reply.TaskID)
		} else {
			log.Printf(prefix+"Map Phase, Begin Task %d", reply.TaskID)
			args.Phase = MAP_PHASE
			filename := reply.FileName

			file, err := os.Open(filename)
			if err != nil {
				args.IsExited = true
				CallReport(&args)
				log.Fatalf("cannot open %v", filename)
			} else {
				log.Printf(prefix+"open %v successful", filename)
			}
			
			content, err := ioutil.ReadAll(file)
			if err != nil {
				args.IsExited = true
				CallReport(&args)
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))

			ofiles := make([]*os.File, reply.NReduces)
			encoders := make([]*json.Encoder, reply.NReduces)
			for i := 0; i < reply.NReduces; i++ {
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
				ofiles[i], _ = os.Create(oname)
				encoders[i] = json.NewEncoder(ofiles[i])
			}
			// write the map key value into file in json format
			for _, kv := range kva {
				index := ihash(kv.Key) % reply.NReduces
				encoders[index].Encode(&kv)
			}
			for i := 0; i < reply.NReduces; i++ {
				ofiles[i].Close()
			}
			
			args.IsFinished = true
			log.Printf(prefix+"Map Phase, Finish Task %d", reply.TaskID)
			CallReport(&args)
		}
	}

}

func CallWorkerRegister() int {
	args := RegisterArgs{}
	reply := RegisterReply{}

	call("Coordinator.WorkerRegister", &args, &reply)

	return reply.WorkerID
}

func CallWorkerGetTask(workerID int) *GetTaskReply {

	args := GetTaskArgs{}
	args.WorkerID = workerID
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return &reply
}

func CallReport(args *ReportArgs) *ReportReply {
	reply := ReportReply{}
	call("Coordinator.Report", &args, &reply)
	return &reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
