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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Work struct {
	WorkerID int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
}

func (w *Work) ExcuteMapTask(filename string, t *Task) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()
	kva := w.mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	ofiles := make([]*os.File, t.NReduces)
	encoders := make([]*json.Encoder, t.NReduces)
	for i := 0; i < t.NReduces; i++ {
		oname := fmt.Sprintf("mr-%d-%d", t.TaskID, i)
		ofiles[i], _ = os.Create(oname)
		encoders[i] = json.NewEncoder(ofiles[i])
	}
	// write the map key value into file in json format
	for _, kv := range kva {
		index := ihash(kv.Key) % t.NReduces
		encoders[index].Encode(&kv)
	}
	for i := 0; i < t.NReduces; i++ {
		ofiles[i].Close()
	}
	return nil
}

func (w *Work) ExcuteReduceTask(t *Task) error {
	maps := make(map[string][]string)
	for i := 0; i < t.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, t.TaskID)

		file, err := os.Open(filename)
		if err != nil {
			return err
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
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	if err := ioutil.WriteFile(mergeName(t.TaskID), []byte(strings.Join(res, "")), 0600); err != nil {
		return err
	}
	return nil
}

func (w *Work) CallRegister() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	call("Coordinator.WorkerRegister", &args, &reply)
	w.WorkerID = reply.WorkerID
}

func (w *Work) CallRequestTask() *GetTaskReply {

	args := GetTaskArgs{WorkerID: w.WorkerID}

	reply := GetTaskReply{}
	call("Coordinator.RequestTask", &args, &reply)
	return &reply
}

func (w *Work) CallReport(t *Task) {

	args := ReportArgs{
		WorkerID:    w.WorkerID,
		TaskID:      t.TaskID,
		Phase:       t.Phase,
		AbnormalEnd: false,
	}
	reply := ReportReply{}
	call("Coordinator.Report", &args, &reply)

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := Work{
		WorkerID: -1,
		reducef:  reducef,
		mapf:     mapf,
	}
	w.CallRegister()
	for {
		reply := w.CallRequestTask()
		if reply.NeedExit {
			break
		}
		t := reply.TaskInfo

		if t.Phase == MAP_PHASE {
			w.ExcuteMapTask(t.FileName, &t)
		} else {
			w.ExcuteReduceTask(&t)
		}

		w.CallReport(&t)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
