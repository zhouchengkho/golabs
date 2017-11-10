package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

// import "errors"
import "container/list"

type entry struct {
	val string
	num int64
}
type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	data map[string]string
	nums map[int64]bool

	logs        list.List
	lastApplied *entry
	avlb        bool
	view        viewservice.View
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.avlb {
		reply.Err = ErrPossibleStale
		return nil
	}
	// already ask primary
	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.view.Backup != "" {
		r := &CheckArgs{pb.view.Viewnum}
		a := &CheckReply{Init}
		ok := call(pb.view.Backup, "PBServer.Check", r, a)
		if !ok || a.Err != OK {
			reply.Err = a.Err
			return nil
		}
	}

	// use mutex to ensure safety
	d, exist := pb.data[args.Key]

	if !exist {
		// not exist
		reply.Err = ErrNoKey
		return nil
	}
	reply.Value = d
	reply.Err = OK
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.

	pb.mu.Lock()
	defer pb.mu.Unlock()
	// client approaching backup
	if pb.me == pb.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	_, exist := pb.nums[args.Num]
	// pb.mu.Unlock()

	reply.Err = OK

	if exist {
		// already dealt
		reply.Err = OK
		return nil
	}

	if pb.view.Primary == pb.me {
		val := pb.writeToDb(args.Key, args.Value, args.Op, args.Num)
		pb.nums[args.Num] = true

		if pb.view.Backup != "" {
			//update backup
			a := &ForwardArgs{PutAppendArgs{args.Key, val, args.Op, args.Num, "PRIMARY"}}
			r := &ForwardReply{Init, ""}
			ok := call(pb.view.Backup, "PBServer.Forward", a, r)
			for r.Err != OK || !ok {
				//rpc failed
				time.Sleep(viewservice.PingInterval)
				ok = call(pb.view.Backup, "PBServer.Forward", a, r)
			}
			// fmt.Printf("b replicated key %s with val % s\n", key, pb.data[key])
		}

	} else {
		// not the primary
		reply.Err = ErrWrongServer
	}

	return nil
	//	if pb.lastApplied != nil && pb.lastApplied.num != args.Num {
	//		// there is still a request pending, reject this
	//		reply.Err = Test
	//		return nil
	//	}
	//	reply.Err = OK
	//	// should keep append order same
	//	// in primary and backup

	//	// prevent the reset problem, try at backup first
	//	// put it in backup before reporting
	//	// primary

	//	if pb.view.Backup != "" {
	//		notStale := pb.check()
	//		if !notStale {
	//			reply.Err = ErrPossibleStale
	//			return nil
	//		}
	//		if pb.lastApplied != nil {
	//			// primary has last apply to deal
	//		} else {
	//			pb.lastApplied = &entry{pb.getOpedValue(args.Key, args.Value, args.Op), args.Num}
	//		}
	//		a := &ForwardArgs{PutAppendArgs{args.Key, pb.lastApplied.val, args.Op, args.Num, "PRIMARY"}}
	//		r := &ForwardReply{Init, pb.lastApplied.val}

	//		ok := call(pb.view.Backup, "PBServer.Forward", a, r)

	//		for !ok {
	//			// backup forward failure
	//			// keep
	//			fmt.Printf("huh what happend ok: %s err: %s\n", ok, r.Err)
	//			reply.Err = r.Err
	//			time.Sleep(viewservice.PingInterval)
	//			ok = call(pb.view.Backup, "PBServer.Forward", a, r)
	//			// ok
	//		}
	//		pb.data[args.Key] = pb.lastApplied.val
	//		pb.nums[args.Num] = true
	//		pb.lastApplied = nil
	//		// pb.mu.Unlock()
	//	} else {
	//		// just primary
	//		pb.writeToDb(args.Key, args.Value, args.Op, args.Num)
	//		pb.nums[args.Num] = true
	//	}
	//	return nil

}

// added function
func (pb *PBServer) check() bool {
	r := &CheckArgs{pb.view.Viewnum}
	a := &CheckReply{Init}
	ok := call(pb.view.Backup, "PBServer.Check", r, a)
	if !ok || a.Err != OK {
		return false
	}
	return true
}
func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	reply.Err = OK
	_, exist := pb.nums[args.Args.Num]

	if exist {
		return nil
	}

	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	// put append op
	// op := args.Args.Op
	key := args.Args.Key
	val := args.Args.Value
	pb.data[key] = val
	pb.nums[args.Args.Num] = true
	// end of put append op

	return nil
}

func (pb *PBServer) Check(args *CheckArgs, reply *CheckReply) error {
	reply.Err = OK
	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
	}
	if pb.view.Viewnum != pb.view.Viewnum {
		reply.Err = ErrPossibleStale
	}
	return nil

}

func (pb *PBServer) getOpedValue(key string, val string, op string) string {
	res := ""
	if op == "Put" {
		res = val
	} else if op == "Append" {
		d, ok := pb.data[key]
		if !ok {
			// not exist
			res = val
		} else {
			// already exist
			res = d + val
		}
	}
	return res
}

func (pb *PBServer) Apply(args *ApplyArgs, reply *ApplyReply) error {
	return nil
}

// gen args
func genArgs(args *PutAppendArgs) PutAppendArgs {
	a := PutAppendArgs{args.Key, args.Value, args.Op, args.Num, args.From}
	return a
}

// end of gen args
// write to db
func (pb *PBServer) writeToDb(key string, val string, op string, num int64) string {
	if op == "Put" {
		pb.data[key] = val
	} else if op == "Append" {
		d, ok := pb.data[key]
		if !ok {
			// not exist
			pb.data[key] = val
		} else {
			// already exist
			pb.data[key] = d + val
		}
	}
	return pb.data[key]
}

// end of write to db
// for backup init
func (pb *PBServer) Init(args *InitArgs, reply *InitReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	reply.Err = OK
	if pb.me == pb.view.Backup {
		pb.data = args.Map
		pb.nums = args.Nums
	} else {
		reply.Err = ErrWrongServer
	}
	return nil
}

func (pb *PBServer) printData() {
	for k, v := range pb.data {
		fmt.Printf("key: %s, value: %s \n", k, v)
	}
}

// end of added function

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.

	// pb.mu.Lock()
	// defer pb.mu.Unlock()

	view, ok := pb.vs.Ping(pb.view.Viewnum)
	if ok != nil {
		pb.avlb = false
		return
	}

	pb.avlb = true

	if view != pb.view {
		pb.view = view
		if view.Primary == pb.me && view.Backup != "" {

			args := &InitArgs{pb.data, pb.nums}
			reply := &InitReply{Init}
			ok := call(view.Backup, "PBServer.Init", args, reply)
			for !ok || reply.Err != OK {
				time.Sleep(viewservice.PingInterval)
				ok = call(view.Backup, "PBServer.Init", args, reply)
			}
		}
	}
	// view, err := pb.vs.Ping(pb.view.Viewnum)
	// fmt.Printf("tick: p: %s, b: %s, n: %d \n", view.Primary, view.Backup, view.Viewnum)
	//	if err == nil {
	//		// before updating view, see whats going on first
	//		if view.Viewnum > pb.view.Viewnum {
	//			// moving to a new view
	//			if pb.me == view.Primary {
	//				// now i am master
	//				if view.Backup != "" && pb.view.Backup != view.Backup {
	//					// saw backup initialized
	//					// transfer data
	//					pb.mu.Lock()
	//					args := &InitArgs{pb.data, pb.nums}
	//					reply := &InitReply{OK}
	//					pb.mu.Unlock()
	//					ok := call(view.Backup, "PBServer.Init", args, reply)
	//					for !ok || reply.Err != OK {
	//						time.Sleep(viewservice.PingInterval)
	//						fmt.Printf("init failed \n")
	//						ok = call(view.Backup, "PBServer.Init", args, reply)
	//					}
	//					fmt.Printf("backup initialized\n")
	//				} else if pb.view.Backup == view.Primary {
	//					// i have been promoted as primary
	//					fmt.Printf("%s has been promoted to primary\n", pb.me)
	//				}
	//			} else if pb.me == view.Backup && pb.view.Backup != pb.me {
	//			}
	//		}
	//		// update view
	//		pb.mu.Lock()
	//		pb.view = view
	//		pb.mu.Unlock()
	//	} else {
	//		fmt.Printf("tick: server %s can't talk to view service\n", pb.me)
	//	}

}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.

	// added init
	pb.data = make(map[string]string)
	pb.view = viewservice.View{0, "", ""}
	pb.nums = make(map[int64]bool)
	pb.avlb = true
	// end of added init
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
