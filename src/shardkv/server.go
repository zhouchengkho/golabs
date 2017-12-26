package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Id int64
	Name string
	Args Args
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	currentConfig shardmaster.Config
	lastConfig shardmaster.Config
	shards []bool                    // whether or not ith shard is present
	nextTransitionNumber int                // Num of new Config transitioning to, -1 if not transitioning
	// Key/Value State
	lastSeq int
	data map[string]string        
	cache map[string]Reply
	
}

// RPC handler for client get requests
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := kv.getHandler(args)

	reply.Value = r.Value
	reply.Err = r.Err
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := kv.putAppendHandler(args)
	reply.Err = r.Err
	return nil
}

// RPC handler for receiving shard
func (kv *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := kv.receiveShardHandler(args)
	reply.Err = r.Err
	return nil
}

func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.proposeNoOp() // to catchup

	if kv.nextTransitionNumber == -1 {
		if kv.currentConfig.Num == 0 {
			config := kv.sm.Query(1)
			if config.Num == 1 {
				kv.lastConfig = kv.currentConfig
				kv.currentConfig = config
				kv.shards = getShardState(kv.currentConfig.Shards, kv.gid)
				return
			}
			return
		}

		config := kv.sm.Query(-1)
		if config.Num > kv.currentConfig.Num {
			operation := genOp(StartNewConfig, ReConfigStartArgs{})
			seq := kv.agree(operation)

			kv.catchup(seq)
			kv.doOp(seq, operation)
		} else {
		}
	} else {
		kv.sendShardStateToPeer()

		if kv.sendOk() && kv.receiveOk() {
			operation := genOp(EndNewConfig, ReConfigEndArgs{})
			seq := kv.agree(operation)

			kv.catchup(seq)
			kv.doOp(seq, operation)
		}
	}
}

// tell the server to shut itsm down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(ReConfigStartArgs{})
	gob.Register(ReConfigEndArgs{})
	gob.Register(ReceiveShardArgs{})
	gob.Register(SentShardArgs{})
	gob.Register(NoOpArgs{})
	gob.Register(KVPair{})
	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.lastConfig = shardmaster.Config{}
	kv.lastConfig.Groups = map[int64][]string{}
	kv.currentConfig = shardmaster.Config{}
	kv.currentConfig.Groups = map[int64][]string{}
	kv.shards = make([]bool, shardmaster.NShards)
	kv.nextTransitionNumber = -1
	kv.data = map[string]string{}
	kv.cache =  map[string]Reply{}
	kv.lastSeq = -1



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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
