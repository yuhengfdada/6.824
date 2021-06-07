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
// Request for task RPC.
type TaskReqArgs struct {
}

type TaskReqReply struct {
	TaskType      string // Map or Reduce or "Please exit"
	MapTaskNum    int
	MapNReduces   int
	FileName      string // Map: the raw input filename.
	ReduceTaskNum int    // Reduce: the reduce task number.
	ReduceNMaps   int    // Reduce: number of intermediate files per reduce task.
}

// Task finished RPC.
type TaskFinArgs struct {
	MapTaskNum    int // for Map
	ReduceTaskNum int // for reduce
}

type TaskFinReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
