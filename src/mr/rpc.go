package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// TaskKind selects which workload a reply carries. Workers treat it as the
// authoritative current phase; Wait keeps a worker polling without work.
type TaskKind int

const (
	TaskMap TaskKind = iota
	TaskReduce
	TaskWait
	TaskExit
)

// AskTaskRequest carries no fields: the coordinator owns all scheduling state.
type AskTaskRequest struct {
}

// AskTaskReply describes one unit of work. For TaskMap the worker must read
// InputFile, split by key into NReduce intermediate files, and report MapId;
// for TaskReduce it must fold intermediate files 0..NMap-1 with the same
// ReduceId and report it. MapId and ReduceId reuse the same field Id.
type AskTaskReply struct {
	Kind      TaskKind
	Id        int
	InputFile string
	NMap      int
	NReduce   int
}

// FinishTaskRequest tells the coordinator that task Id of kind Kind
// completed. A crashed worker simply never calls this RPC.
type FinishTaskRequest struct {
	Kind TaskKind
	Id   int
}

type FinishTaskReply struct {
}
