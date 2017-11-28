package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

// import "bytes"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Val       string
	Operation string // "Put" or "Append" or "Get" pr "NoOP", only put & append will be handled
	ClientId  int    // unique identification for caching
	ClientSeq int    // hmm, maybe nrand() is enough, but this is probably safer
}

type entry struct {
	applied bool
	// is it necessary to cache response
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	data        map[string]string      // storage
	cache       map[int]map[int]*entry // cache
	lastApplied int                    // last applied sequence
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// key, value, op, client id, client seq
	operation := Op{args.Key, "", "Get", args.ClientId, args.ClientSeq}

	// get seq for this op
	seq, op := kv.getSeqForOp(operation)

	// catch up until this seq before return
	kv.catchup(seq)

	// now, try get value
	v, err := kv.getValue(seq, op)
	reply.Err = err
	reply.Value = v

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// key, val, op, client id, client seq
	op := Op{args.Key, args.Value, args.Op, args.ClientId, args.ClientSeq}

	// should decide seq number here
	kv.getSeqForOp(op)

	// until getting a value, don't have to apply yet

	// just return ok
	reply.Err = OK
	return nil

}

// added function

func (kv *KVPaxos) getSeqForOp(operation Op) (int, Op) {
	var seq int
	var decidedOp Op

	for decidedOp != operation {
		seq = kv.px.Max() + 1 // next available sequence
		decidedOp = kv.agreeUntilDecide(operation, seq)
	}

	return seq, decidedOp
}

func (kv *KVPaxos) agreeUntilDecide(operation Op, seq int) Op {
	kv.px.Start(seq, operation)

	to := 10 * time.Millisecond

	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			return v.(Op)
		}
		time.Sleep(to)

		if to < 10*time.Second {
			to *= 2
		}

	}
}

func (kv *KVPaxos) catchup(seq int) {
	lastSeq := kv.lastApplied + 1

	status, v := kv.px.Status(lastSeq)

	for status == paxos.Decided && lastSeq <= seq {

		// since decided
		// apply this op
		kv.apply(v.(Op), lastSeq)

		// done, so that paxos rm earlier logs
		kv.px.Done(lastSeq)

		lastSeq = kv.lastApplied + 1
		status, v = kv.px.Status(lastSeq)

	}
}

func (kv *KVPaxos) apply(operation Op, seq int) {
	key := operation.Key
	val := operation.Val
	op := operation.Operation

	// last applied increases to this
	kv.lastApplied = seq

	// create cache
	kv.createCache(operation.ClientId, operation.ClientSeq)

	// check if in cache
	if kv.cache[operation.ClientId][operation.ClientSeq].applied {
		// job done
	} else {
		// apply it
		if op == "Put" {
			kv.data[key] = val
		} else if op == "Append" {
			v, exist := kv.data[key]

			if exist {
				v = v + val
				kv.data[key] = v
			} else {
				kv.data[key] = val
			}

		}
	}

	// change applied
	kv.cache[operation.ClientId][operation.ClientSeq].applied = true

}

func (kv *KVPaxos) createCache(clientId int, clientSeq int) {
	// cache it
	_, exist := kv.cache[clientId]
	if exist {
		_, exist := kv.cache[clientId][clientSeq]
		if exist {
			// already cached
			return
		} else {
			kv.cache[clientId][clientSeq] = &entry{false}
		}
	} else {
		kv.cache[clientId] = make(map[int]*entry)
		kv.cache[clientId][clientSeq] = &entry{false}
	}

}

func (kv *KVPaxos) getValue(seq int, op Op) (string, Err) {
	for {
		if kv.lastApplied >= seq {
			// safe to return
			v, exist := kv.data[op.Key]
			if exist {
				return v, OK
			} else {
				return "", ErrNoKey
			}
		}

		// add no op to paxos sequence
		// key, val, op, client id, client seq
		noOp := Op{"", "", "NOOP", -1, -1}
		noSeq := kv.lastApplied + 1
		if noSeq >= seq {
			noSeq = seq
		}
		kv.agreeUntilDecide(noOp, noSeq)
		kv.catchup(noSeq)

	}
}

// end of added function

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.cache = make(map[int]map[int]*entry)
	kv.lastApplied = -1

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
