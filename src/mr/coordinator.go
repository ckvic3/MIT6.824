package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Phase int

type Task struct {
	FileName  string
	TaskID    int
	TaskPhase Phase
	State     int // 代表Task当前的状态
}

type Coordinator struct {
	// Your definitions here.
	mutex           sync.Mutex
	NMap            int
	NReduce         int
	phase           Phase
	numOfWorker     int
	MapTaskState    map[int]int
	MapWorkerID     map[int]int
	FileID          map[string]int
	FileName        map[int]string
	ReduceTaskState map[int]int
	ReduceWorkerID  map[int]int
	AllIsFinished   bool
	AllWokerExit    bool
	WorkerHasExited map[int]bool
	MAX_EXCUTE_TIME time.Duration
	FinishAll       bool
	TaskQueue       chan int // 在map 阶段, 任务以file对应的编号的代表，从 0 到 n-1. 在reduce 阶段，任务以0到9编号。
}

// Your code here -- RPC handlers for the worker to call.

// supply the remote service of Register a worker
// note: the worker number will keep increase, and is an atomic variable
func (c *Coordinator) WorkerRegister(args *RegisterArgs, reply *RegisterReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.WorkerID = c.numOfWorker
	c.WorkerHasExited[reply.WorkerID] = false
	c.numOfWorker += 1
	return nil
}

// 远程worker 调用该服务获取一个需要执行的任务，当master节点的 所有阶段执行完毕标志置为true时
// 对所有继续请求服务的worker,回复信息中 AllIsFinished 标志置为真，通知worker 自动退出。
// 否则，从task队列中找到一个任务分配该worker
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	reply.NReduces = c.NReduce
	reply.NeedExit = true
	reply.NMap = c.NMap
	reply.MapPhaseIsFinished = false
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.FinishAll {
		if !c.AllWokerExit {
			c.WorkerHasExited[args.WorkerID] = true
			c.AllWokerExit = true
			for i := 0; i < c.numOfWorker; i++ {
				if !c.WorkerHasExited[i] {
					c.AllWokerExit = false
					break
				}
			}
		}
		return nil
	}

	id, ok := <-c.TaskQueue
	if !ok {
		return nil
	} else {
		log.Printf("Master Node: apply Task %v", id)
	}

	reply.TaskID = id
	reply.NeedExit = false
	reply.MapPhaseIsFinished = false

	// 放入在准备队列中的任务，除了分配任务时，不会再有更改状态
	// 对于map阶段
	if c.phase == MAP_PHASE {
		reply.FileName = c.FileName[reply.TaskID]
		c.MapTaskState[reply.TaskID] = RUNING
		c.MapWorkerID[reply.TaskID] = args.WorkerID

	} else {
		c.ReduceTaskState[reply.TaskID] = RUNING
		c.ReduceWorkerID[reply.TaskID] = args.WorkerID
		reply.MapPhaseIsFinished = true

	}
	log.Printf("Master Node: give worker %d, task %d ", args.WorkerID, reply.TaskID)

	// 设置定时器，检测该Task是否在正常时间内完成
	go c.checkTaskIsAlive(reply.TaskID, int(c.phase), int(args.WorkerID))
	return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// map 阶段
	if args.Phase == MAP_PHASE {
		// worker 异常退出, 将原来的任务设置为NOT_READY 状态
		if args.IsExited {
			c.MapTaskState[args.TaskID] = NOT_READY
			c.MapWorkerID[args.TaskID] = -1
			c.WorkerHasExited[args.WorkID] = true
			log.Printf("Master Node: worker %v exit Task %v unexpected!", args.WorkID, args.TaskID)
		} else if args.IsFinished { // 完成当前任务，修正master节点对任务的状态
			// 只有Task 对应的worker ID 与报告的wokre id 相同，且该任务处于running 状态时，才能确定更新状态
			if c.MapTaskState[args.TaskID] == RUNING && args.WorkID == c.MapWorkerID[args.TaskID] {
				c.MapTaskState[args.TaskID] = FINISHED
				log.Printf("Master Node: worker %v finished Task %v!", args.WorkID, args.TaskID)
			}
		}
	} else {
		if args.IsExited {
			c.ReduceTaskState[args.TaskID] = NOT_READY
			c.ReduceWorkerID[args.TaskID] = -1
			c.WorkerHasExited[args.WorkID] = true
			log.Printf("Master Node: worker %v exit Task %v unexpected!", args.WorkID, args.TaskID)
		} else if args.IsFinished {
			if c.ReduceTaskState[args.TaskID] == RUNING && args.WorkID == c.ReduceWorkerID[args.TaskID] {
				c.ReduceTaskState[args.TaskID] = FINISHED
				log.Printf("Master Node: worker %v finished Task %v!", args.WorkID, args.TaskID)
			}
		}
	}

	c.schedule()
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

// 在sleep 设置的最大任务执行事件后，检查任务是否完成
func (c *Coordinator) checkTaskIsAlive(TaskID int, TaskPahse int, workerID int) {

	time.Sleep(time.Duration(60 * time.Second))
	
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if TaskPahse == MAP_PHASE {
		// 检查该任务是否已经分配给其他的worker。如果已经分配，就没有检查超时的必要了
		if c.MapWorkerID[TaskID] != workerID {
			return
		}
		if c.MapTaskState[TaskID] == FINISHED {
			return
		}
		if c.MapTaskState[TaskID] == RUNING {
			c.MapTaskState[TaskID] = NOT_READY
		}
	} else {
		if c.ReduceWorkerID[TaskID] != workerID {
			return
		}
		if c.ReduceTaskState[TaskID] == FINISHED {
			return
		}
		if c.ReduceTaskState[TaskID] == RUNING {
			c.ReduceTaskState[TaskID] = NOT_READY
		}
	}
	log.Printf("Master Node: Task %v timeout!", TaskID)
	c.schedule()
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
	// 遍历 MAP task state 和Reduce task state，只有当所有对应的状态为FINISHED时才返回true
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.FinishAll && c.AllWokerExit
}

// 负责周期性的更新Task 状态
func (c *Coordinator) schedule() {
	// 1、根据map 和 reduce 两个不同阶段划分任务
	c.AllIsFinished = true
	if c.phase == MAP_PHASE {
		for i := 0; i < len(c.MapTaskState); i++ {
			switch c.MapTaskState[i] {
			case NOT_READY:
				c.TaskQueue <- i
				fmt.Printf("phase 1, push Task %d \n", i)
				c.MapTaskState[i] = READY
				c.AllIsFinished = false
			case READY:
				c.AllIsFinished = false
			case RUNING:
				c.AllIsFinished = false
			case FINISHED:
			default:
				fmt.Println("unexpected state!")
			}
		}
	} else {

		for i := 0; i < len(c.ReduceTaskState); i++ {
			switch c.ReduceTaskState[i] {
			case NOT_READY:
				c.TaskQueue <- i
				c.ReduceTaskState[i] = READY
				c.AllIsFinished = false
				fmt.Printf("phase 2, push Task %d \n", i)
			case READY:
				c.AllIsFinished = false
			case RUNING:
				c.AllIsFinished = false
			case FINISHED:
			default:
				fmt.Println("unexpected state!")
			}
		}
	}
	if c.AllIsFinished && c.phase == REDUCE_PHASE {
		c.FinishAll = true
	} else if c.AllIsFinished && c.phase == MAP_PHASE {
		c.phase = REDUCE_PHASE
		c.schedule()
	}

	c.AllWokerExit = true
	for i := 0; i < c.numOfWorker; i++ {
		if !c.WorkerHasExited[i] {
			c.AllWokerExit = false
			break
		}
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
	// 初始化代码
	c.AllIsFinished = false
	c.FinishAll = false
	c.MAX_EXCUTE_TIME = 40
	c.phase = MAP_PHASE
	c.NReduce = nReduce
	c.NMap = len(files)

	c.MapTaskState = make(map[int]int)
	c.FileName = make(map[int]string, len(files))
	c.FileID = make(map[string]int, len(files))
	c.MapWorkerID = make(map[int]int)

	c.ReduceTaskState = make(map[int]int, nReduce)
	c.ReduceWorkerID = make(map[int]int)

	c.WorkerHasExited = make(map[int]bool)
	c.TaskQueue = make(chan int, len(files)+20)
	var id int = 0
	// 建立文件名与 Task ID 之间的映射关系，初始化各个任务的状态
	for _, file := range files {
		c.FileName[id] = file
		c.MapTaskState[id] = NOT_READY
		c.FileID[file] = id
		c.MapWorkerID[id] = -1
		id = id + 1
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskState[i] = NOT_READY
		c.ReduceWorkerID[i] = -1
	}
	c.server()
	c.schedule()

	fmt.Printf("init is finished!")
	return &c
}
