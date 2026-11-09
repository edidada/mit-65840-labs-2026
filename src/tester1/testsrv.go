package tester

import (
	//"log"
	"log"
	"sync"
	"time"

	"6.5840/tester1/sockrpc"
)

type TesterRPC struct {
	*sockrpc.RPCSrv
	cfg *Config
}

// Make a RPC server for the tester to receive RPCs from daemons
func newTesterRPCSrv(cfg *Config) *TesterRPC {
	trpc := &TesterRPC{
		cfg: cfg,
	}
	//log.Printf("newTesterRPCSrv %v", cfg.endName)
	trpc.RPCSrv = sockrpc.NewRPCSrv(cfg.endName)
	trpc.RPCSrv.AddService(trpc)
	return trpc
}

func (trpc *TesterRPC) cleanup() {
	//log.Printf("TesterRPCsrv: close %v", trpc.cfg.endName)
	trpc.Close()
}

type ForwardArgs struct {
	Method string
	End    string // client end for server
	Args   []byte
	Id     int64
}

type ForwardReply struct {
	Rep []byte
	Ok  bool
}

// Forward RPC to a deamon through the lab net
func (trpc *TesterRPC) Forward(args *ForwardArgs, reply *ForwardReply) {
	//log.Printf("%v: Forward args %v to end %q %d", trpc.Name(), args.Method, args.End, args.Id)
	end := trpc.cfg.net.LookupEnd(args.End)
	if end == nil {
		log.Printf("[fwdmon] nil end %v method %v", args.End, args.Method)
		reply.Rep, reply.Ok = nil, false
		return
	}
	fwdPendAdd(args.End)
	start := time.Now()
	reply.Rep, reply.Ok = end.Forward(args.Method, args.Args)
	fwdPendDone(args.End)
	if d := time.Since(start); d > 2*time.Second {
		log.Printf("[fwdmon back end=%v method=%v took=%v ok=%v]", args.End, args.Method, d.Truncate(time.Second), reply.Ok)
	}
}

var (
	fwdPendMu   sync.Mutex
	fwdPendMap  = map[string]int64{}
	fwdPendOnce sync.Once
)

func fwdPendAdd(end string) {
	fwdPendMu.Lock()
	fwdPendMap[end]++
	fwdPendMu.Unlock()
	fwdPendOnce.Do(func() {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				fwdPendMu.Lock()
				for end, n := range fwdPendMap {
					if n > 0 {
						log.Printf("[fwdmon backlog end=%v pending=%v]", end, n)
					}
				}
				fwdPendMu.Unlock()
			}
		}()
	})
}

func fwdPendDone(end string) {
	fwdPendMu.Lock()
	fwdPendMap[end]--
	if fwdPendMap[end] == 0 {
		delete(fwdPendMap, end)
	}
	fwdPendMu.Unlock()
}
