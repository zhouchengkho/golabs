package kvpaxos

import "net/rpc"

// import "crypto/rand"

// import "math/big"
import "math/rand"
import "fmt"
import "time"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	leader string
	i      int

	id      int // client unique id
	baseSeq int // track how many sent
}

//func nrand() int64 {
//	max := big.NewInt(int64(1) << 62)
//	bigx, _ := rand.Int(rand.Reader, max)
//	x := bigx.Int64()
//	return x
//}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	// will assume servers will be at least one
	// choose the first one as default leader
	// retry at another one if request failed
	ck.leader = servers[0]
	ck.i = 0

	ck.id = rand.Int()
	ck.baseSeq = -1

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.baseSeq++
	// key, client id, client seq
	args := &GetArgs{key, ck.id, ck.baseSeq}

	var reply GetReply
	ok := call(ck.leader, "KVPaxos.Get", args, &reply)
	for !ok || reply.Err != OK {

		if reply.Err == ErrNoKey {
			return ""
		}
		time.Sleep(100 * time.Millisecond)

		ck.changeLeader()
		ok = call(ck.leader, "KVPaxos.Get", args, &reply)

	}
	if reply.Err == OK {
		return reply.Value
	}

	return ""
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.baseSeq++

	// key, value, op, client id, client seq
	args := &PutAppendArgs{key, value, op, ck.id, ck.baseSeq}
	var reply PutAppendReply
	ok := call(ck.leader, "KVPaxos.PutAppend", args, &reply)
	for !ok || reply.Err != OK {
		time.Sleep(100 * time.Millisecond)

		ck.changeLeader()
		ok = call(ck.leader, "KVPaxos.PutAppend", args, &reply)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// added function
func (ck *Clerk) changeLeader() {
	if ck.i+1 < len(ck.servers) {
		ck.i++
	} else {
		ck.i = 0
	}
	ck.leader = ck.servers[ck.i]
}

// end of added function
