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

type Master struct {
	// Your definitions here.
	// ReduceFiles []string
	Phase   string // Map or Reduce or Done
	Files   []string
	NMap    int
	NReduce int

	NAllJobs      int
	NFreeJobs     int
	NFinishedJobs int
	States        map[int]string
	ReduceStates  map[int]string

	OutstdngReqs int // used in Done()
	mutex        sync.Mutex
	Timers       map[int]*time.Timer
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) TaskReqHandler(args *TaskReqArgs, reply *TaskReqReply) error {
	m.mutex.Lock()
	m.OutstdngReqs += 1
	m.mutex.Unlock()
	for {
		m.mutex.Lock()
		if m.NFreeJobs > 0 { // Give task when there is a free job.
			if m.Phase == "Map" {
				for taskNum, state := range m.States {
					if state == "free" {
						reply.TaskType = m.Phase
						reply.FileName = m.Files[taskNum]
						reply.MapTaskNum = taskNum
						reply.MapNReduces = m.NReduce
						m.NFreeJobs -= 1
						m.States[taskNum] = "in-progress"
						m.Timers[taskNum] = time.NewTimer(10 * time.Second)
						go func(mm *Master, tN int) {
							<-mm.Timers[tN].C
							mm.mutex.Lock()
							if mm.States[tN] == "in-progress" {
								mm.States[tN] = "free"
								mm.NFreeJobs += 1
							}
							mm.mutex.Unlock()
						}(m, taskNum)
						m.OutstdngReqs -= 1
						m.mutex.Unlock()
						return nil
					}
				}
			} else {
				for taskNum, state := range m.ReduceStates {
					if state == "free" {
						reply.TaskType = m.Phase
						reply.FileName = ""
						reply.ReduceTaskNum = taskNum
						reply.ReduceNMaps = m.NMap
						m.NFreeJobs -= 1
						m.ReduceStates[taskNum] = "in-progress"
						m.Timers[taskNum] = time.NewTimer(10 * time.Second)
						go func(mm *Master, tN int) {
							<-mm.Timers[tN].C
							mm.mutex.Lock()
							if mm.ReduceStates[tN] == "in-progress" {
								mm.ReduceStates[tN] = "free"
								mm.NFreeJobs += 1
							}
							mm.mutex.Unlock()
						}(m, taskNum)
						m.OutstdngReqs -= 1
						m.mutex.Unlock()
						return nil
					}
				}
			}
		} else { // When no free task, wait.
			if m.Phase == "Done" { // If everything is done, tell the workers to exit.
				reply.TaskType = "Please exit"
				reply.FileName = ""
				m.OutstdngReqs -= 1
				m.mutex.Unlock()
				return nil
			} else {
				m.mutex.Unlock() // give chance to other threads
				time.Sleep(3 * time.Second)
			}
		}
	}
}

func (m *Master) TaskFinHandler(args *TaskFinArgs, reply *TaskFinReply) error {
	// Update master metadata.
	m.mutex.Lock()
	if m.Phase == "Done" {
		m.mutex.Unlock()
		return nil
	}
	if m.Phase == "Map" {
		m.States[args.MapTaskNum] = "finished"
	} else {
		m.ReduceStates[args.ReduceTaskNum] = "finished"
	}
	m.NFinishedJobs += 1
	/*
		if m.Phase == "Map" { // record the reduce files. (is it necessary?)
			m.ReduceFiles = append(m.ReduceFiles, args.ReduceFileNames...)
		}
	*/
	// Check whether the phase has finished. If so, go to the next phase.
	if m.NAllJobs == m.NFinishedJobs {
		if m.Phase == "Map" {
			m.NAllJobs = m.NReduce
			m.NFinishedJobs = 0
			m.NFreeJobs = m.NAllJobs
			for i := 0; i < m.NReduce; i++ {
				m.ReduceStates[i] = "free"
			}
			m.Phase = "Reduce"
		} else {
			m.Phase = "Done"
		}
	}
	m.mutex.Unlock()
	return nil
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
	if m.Phase == "Done" && m.OutstdngReqs == 0 {
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
	m.NMap = len(files)

	m.NAllJobs = m.NMap
	m.NFinishedJobs = 0
	m.NFreeJobs = m.NAllJobs
	m.States = make(map[int]string)
	m.ReduceStates = make(map[int]string)
	for i := 0; i < m.NMap; i++ {
		m.States[i] = "free"
	}
	m.Timers = make(map[int]*time.Timer)
	m.Phase = "Map"
	m.OutstdngReqs = 0

	m.server()
	return &m
}
