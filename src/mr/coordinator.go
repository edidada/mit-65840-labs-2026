package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

const revokeAfter = 10 * time.Second
const supervisorTick = 500 * time.Millisecond

// taskState tracks one scheduled unit: assigned registers an in-flight
// attempt with its start time, so a crashed worker is reissued for free.
type taskState struct {
	input    string
	finished bool
	assigned bool
	since    time.Time
}

type Coordinator struct {
	mu sync.Mutex

	mapStates    []*taskState
	reduceStates []*taskState
	nReduce      int

	live bool
}

// Task assignments and completions are the whole scheduling surface. A worker
// that stops answering is detected by since and simply reissued, matching the
// lab's allowance for duplicate map work.
func (c *Coordinator) AskTask(args *AskTaskRequest, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.NReduce = c.nReduce
	reply.NMap = len(c.mapStates)
	now := time.Now()
	for id, st := range c.mapStates {
		if st.finished {
			continue
		}
		if !st.assigned {
			st.assigned = true
			st.since = now
			reply.Kind = TaskMap
			reply.Id = id
			reply.InputFile = st.input
			return nil
		}
	}
	mapsFinished := true
	for _, st := range c.mapStates {
		if !st.finished {
			mapsFinished = false
			break
		}
	}
	if mapsFinished {
		for id, st := range c.reduceStates {
			if st.finished {
				continue
			}
			if !st.assigned {
				st.assigned = true
				st.since = now
				reply.Kind = TaskReduce
				reply.Id = id
				return nil
			}
		}
		reducesFinished := true
		for _, st := range c.reduceStates {
			if !st.finished {
				reducesFinished = false
				break
			}
		}
		if reducesFinished {
			reply.Kind = TaskExit
			return nil
		}
	}
	reply.Kind = TaskWait
	return nil
}

// FinishTaskRequest marks one task complete regardless of which worker
// finished it; duplicates are accepted because revoked tasks can still
// complete.
func (c *Coordinator) FinishTask(args *FinishTaskRequest, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var states []*taskState
	switch args.Kind {
	case TaskMap:
		states = c.mapStates
	case TaskReduce:
		states = c.reduceStates
	default:
		return nil
	}
	if args.Id < 0 || args.Id >= len(states) {
		return nil
	}
	states[args.Id].finished = true
	states[args.Id].assigned = false
	return nil
}

// watchdog reissues tasks whose worker stopped reporting beforeEach the
// revoke window elapsed. Reassignment is idempotent because map workers
// always write intermediates through atomic renames.
func (c *Coordinator) watchdog() {
	tick := time.NewTicker(supervisorTick)
	defer tick.Stop()
	for range tick.C {
		c.mu.Lock()
		if !c.live {
			c.mu.Unlock()
			return
		}
		now := time.Now()
		for _, st := range c.mapStates {
			if st.assigned && !st.finished && now.Sub(st.since) > revokeAfter {
				st.assigned = false
			}
		}
		for _, st := range c.reduceStates {
			if st.assigned && !st.finished && now.Sub(st.since) > revokeAfter {
				st.assigned = false
			}
		}
		c.mu.Unlock()
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	mapsFinished := true
	reducesFinished := true
	for _, st := range c.mapStates {
		if !st.finished {
			mapsFinished = false
			break
		}
	}
	for _, st := range c.reduceStates {
		if !st.finished {
			reducesFinished = false
			break
		}
	}
	return mapsFinished && reducesFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	for _, file := range files {
		c.mapStates = append(c.mapStates, &taskState{input: file})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceStates = append(c.reduceStates, &taskState{})
	}
	c.live = true
	go c.watchdog()
	c.server(sockname)
	return &c
}
