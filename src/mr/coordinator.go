package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 一个任务被 worker A执行，超时后，被分配给另一个Worker B。而Worker A完成后，修改任务状态为完成，而Worker B仍在执行该任务。
// 导致状态的不一致。

// 解决方案：为每个任务记录对应的worker编号。在report时，如果任务完成而且任务处于运行状态，且编号一致，则修改。
// 或者任务处于ready状态或者not-Ready状态也进行修改。否则不进行修改。
type Coordinator struct {
	// Your definitions here.
	mutex           sync.Mutex
	WorkerNum       int
	ClosedWorkerNum int
	WorkerExited    map[int]bool
	TaskDone        bool
	WorkerDone      bool
	TaskQueue       chan int
	FileNames       map[int]string
	NMaps           int
	NReduces        int
	Phase           int
	TaskState       map[int]int
	Task2Worker     map[int]int
	MAX_EXCUTE_TIME time.Duration
	closed          bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) WorkerRegister(args *RegisterArgs, reply *RegisterReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.WorkerID = c.WorkerNum
	c.WorkerExited[c.WorkerNum] = false
	c.WorkerNum += 1

	return nil
}

func (c *Coordinator) RequestTask(args *GetTaskArgs, reply *GetTaskReply) error {

	reply.NeedExit = true
	for {
		id, ok := <-c.TaskQueue
		// 当TaskQueue关闭,代表所有的任务都已经执行完毕。
		if !ok {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			c.WorkerExited[args.WorkerID] = true
			c.ClosedWorkerNum++
			c.WorkerDone = c.WorkerNum == c.ClosedWorkerNum
			return nil
		}
		reply.NeedExit = false

		ret := c.GetTask(id, args.WorkerID)
		if ret != nil {
			reply.TaskInfo = *ret
			break
		}
		log.Print("return nil!")
	}
	return nil
}

func (c *Coordinator) GetTask(id int, WorkerID int) *Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.TaskState[id] == FINISHED {
		return nil
	}
	t := Task{
		NReduces: c.NReduces,
		NMap:     c.NMaps,
		TaskID:   id,
		Phase:    c.Phase,
	}
	if t.Phase == MAP_PHASE {
		t.FileName = c.FileNames[id]

	}
	c.TaskState[id] = RUNING
	c.Task2Worker[id] = WorkerID
	go c.checkTaskIsAlive(t.TaskID, t.Phase, WorkerID)
	return &t
}

func (c *Coordinator) checkTaskIsAlive(TaskID int, TaskPahse int, WorkerID int) {
	time.Sleep(c.MAX_EXCUTE_TIME * time.Second)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if TaskPahse != c.Phase {
		return
	}

	if c.TaskState[TaskID] == FINISHED {
		return
	} else {
		c.TaskState[TaskID] = NOT_READY
		c.WorkerExited[WorkerID] = true
	}

	go c.schedule()
}

func (c *Coordinator) schedule() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	AllIsFinished := true
	for i := 0; i < len(c.TaskState); i++ {
		switch c.TaskState[i] {
		case NOT_READY:
			c.TaskQueue <- i
			c.TaskState[i] = READY
			log.Printf("Push Task %v", i)
			AllIsFinished = false
		case READY:
			AllIsFinished = false
		case RUNING:
			AllIsFinished = false
		case FINISHED:
		default:
		}
	}
	if AllIsFinished {
		if c.Phase == MAP_PHASE {
			log.Printf("Begin Reduce Phase")
			c.Phase = REDUCE_PHASE
			// 清空缓存
			for {
				select {
				case <-c.TaskQueue:
				default:
					goto end
				}
			}
		end:
			ReduceInit(c)
			// 开启新的规划进程
			go c.schedule()
		} else {
			c.TaskDone = true
		}
	}
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.WorkerExited[args.WorkerID] = false
	// 对于提前退出的进程，更新进程记录状态
	if args.AbnormalEnd {
		c.WorkerExited[args.WorkerID] = true
		c.ClosedWorkerNum++
		c.WorkerDone = c.WorkerNum == c.ClosedWorkerNum
		return nil
	}
	if args.Phase != c.Phase {
		return nil
	}

	if c.Task2Worker[args.TaskID] == args.WorkerID {
		log.Printf("Task %v finished", args.TaskID)
		c.TaskState[args.TaskID] = FINISHED
	} else if c.TaskState[args.TaskID] == NOT_READY || c.TaskState[args.TaskID] == READY {
		c.TaskState[args.TaskID] = FINISHED
		log.Printf("Task %v finished", args.TaskID)
	}

	go c.schedule()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.TaskDone && !c.closed {
		close(c.TaskQueue)
		c.closed = true
	}
	return c.TaskDone && c.WorkerDone
}

func MapInit(c *Coordinator) {
	c.TaskState = make(map[int]int, len(c.FileNames))
	c.Task2Worker = make(map[int]int, len(c.FileNames))
	for k := range c.FileNames {
		c.TaskState[k] = NOT_READY
		c.Task2Worker[k] = -1
	}
}

func ReduceInit(c *Coordinator) {
	c.TaskState = make(map[int]int, 10)
	c.Task2Worker = make(map[int]int, 10)
	for i := 0; i < 10; i++ {
		c.TaskState[i] = NOT_READY
		c.Task2Worker[i] = -1
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MAX_EXCUTE_TIME = 40

	c.NMaps = len(files)
	c.NReduces = 10
	c.Phase = MAP_PHASE

	c.FileNames = make(map[int]string, len(files))
	c.TaskQueue = make(chan int, 10)
	c.WorkerExited = make(map[int]bool)

	c.WorkerNum = 0
	c.WorkerDone = false
	c.TaskDone = false
	c.ClosedWorkerNum = 0
	c.closed = false

	for i, file := range files {
		c.FileNames[i] = file
	}

	MapInit(&c)
	log.Printf("Task Number: %v", len(c.TaskState))
	c.schedule()
	c.server()
	return &c
}
