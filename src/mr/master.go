package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Master struct {
	// Your definitions here.
	Phase   string // Map or Reduce or Done
	Files   []string
	NMap    int
	NReduce int

	NAllJobs      int
	NFreeJobs     int
	NFinishedJobs int
	States        map[string]string
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) TaskReqHandler(args *TaskReqArgs, reply *TaskReqReply) error {
	for {
		if m.NFreeJobs > 0 { // Give task when there is a free job.
			for fileName, state := range m.States {
				if state == "free" {
					reply.TaskType = m.Phase
					reply.FileName = fileName
					m.NFreeJobs -= 1
					m.States[fileName] = "in-progress"
					return nil
				}
			}
		} else { // When no free task, wait.
			if m.Phase == "Done" { // If everything is done, tell the workers to exit.
				reply.TaskType = "Please exit"
				reply.FileName = nil
				return nil
			} else {
				time.Sleep(3)
			}
		}
	}
	return nil
}

func (m *Master) TaskFinHandler(args *TaskFinArgs, reply *TaskFinReply) error {
	// Update master metadata.
	m.States[args.FileName] = "finished"
	m.NFinishedJobs += 1
	// check whether the phase has finished.
	if m.NAllJobs == m.NFinishedJobs {
		if m.Phase == "Map" {

		}
	}

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.Phase == "Done" {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// Master Initialization.
	m.Files = files
	m.NReduce = nReduce
	m.LatestWID = 0

	m.server()
	return &m
}
