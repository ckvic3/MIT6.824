package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	MAP_PHASE = 0
	REDUCE_PHASE = 1
	NOT_READY = 2
	READY = 3
	RUNING = 4
	FINISHED = 5
)


type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerID int		 
}


type GetTaskArgs struct {
	WorkerID int	// 
}

type GetTaskReply struct {
	FileName string
	TaskID int
	NReduces int
	NMap	int
	MapPhaseIsFinished bool
	NeedExit bool	// when all worker finished, Exit
}

type ReportArgs struct {
	Phase int	// 当前任务处于的任务阶段, 有mapPhase 和 reducePhase 两种阶段
	WorkID int			// worker 在 master处注册的ID标识
	IsFinished bool		// 分配给worker的任务是否完成
	TaskID int			// 对于map任务,代表对应处理的文件的编号. 对于reduce任务,代表其对应的任务编号
	IsExited bool		// 当worker出现异常退出时,该标志标记为true,否则为false
}

type ReportReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
