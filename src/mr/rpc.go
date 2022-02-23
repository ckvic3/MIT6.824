package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	MAP_PHASE    = 0
	REDUCE_PHASE = 1
	NOT_READY    = 2
	READY        = 3
	RUNING       = 4
	FINISHED     = 5
)

type Task struct {
	FileName string
	NReduces int
	NMap     int
	TaskID   int
	Phase    int
}

type RegisterArgs struct {
}
type RegisterReply struct {
	WorkerID int
}
type GetTaskArgs struct {
	WorkerID int
}
type GetTaskReply struct {
	TaskInfo Task
	NeedExit bool
}

type ReportArgs struct {
	Phase       int
	WorkerID    int
	TaskID      int
	AbnormalEnd bool
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
