package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"
import "container/list"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	view          View
	clients       map[string]time.Time
	pool          *list.List
	acked         bool
	primaryPinged time.Time
	backupPinged  time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if args.Me == vs.view.Primary {
		// check ack
		if args.Viewnum == vs.view.Viewnum {
			vs.acked = true
		}

		if args.Viewnum == 0 {
			// primary restarted
			if vs.view.Backup != "" {
				vs.toNewView(vs.view.Backup, "")
			}
		}

	} else if args.Me == vs.view.Backup {

	} else if vs.inPool(args.Me) {

	} else {
		//unknown server pinging
		if args.Viewnum == 0 && vs.view.Primary == "" {
			// initialization
			vs.toNewView(args.Me, "")
		} else if args.Viewnum == 0 {
			// sending zero, but primary is not empty
			// remember this is a unknown server, meaning not a crashed server
			// so in this case should be intializing too, but since primary is not empty
			// this server can be backup, if primary has acked
			// if backup already filled, put this in pool

			if vs.view.Backup == "" && vs.acked {
				vs.toNewView(vs.view.Primary, args.Me)
			} else {
				// put it in pool
				if !vs.inPool(args.Me) {
					vs.pool.PushBack(args.Me)
				}
			}
		}
	}

	vs.updateTime(args)

	reply.View = vs.view
	// fmt.Printf("vs: reply: %s %s %d \n", vs.view.Primary, vs.view.Backup, vs.view.Viewnum)

	return nil
}

// self defined helper function
func (vs *ViewServer) inPool(me string) bool {
	for e := vs.pool.Front(); e != nil; e = e.Next() {
		if e.Value.(string) == me {
			return true
		}
	}
	return false
}

func (vs *ViewServer) toNewView(primary string, backup string) {
	vs.view.Viewnum = vs.view.Viewnum + 1
	if vs.view.Backup == primary {
		// changing it to primary
		t := vs.primaryPinged
		vs.primaryPinged = vs.backupPinged
		vs.backupPinged = t
	}
	vs.view.Primary = primary
	vs.view.Backup = backup
	vs.acked = false
}

func (vs *ViewServer) updateTime(args *PingArgs) {
	vs.clients[args.Me] = time.Now()
}

func (vs *ViewServer) tryFetchFromPool() string {
	if vs.pool.Len() == 0 {
		return ""
	} else {
		e := vs.pool.Front()
		s := e.Value.(string)
		vs.pool.Remove(e)
		return s
	}
}

// end of self defined helper function

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	// return the viewnum, primary, Backup
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	// keep no ping count
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.view.Primary == "" {
		// primary has not been decided
		return
	}

	primaryTime, _ := vs.clients[vs.view.Primary]

	if primaryTime.Add(PingInterval * DeadPings).Before(time.Now()) {
		// primary dead
		if vs.view.Backup != "" && vs.acked {
			vs.acked = false
			vs.view.Primary = ""
			// change to a new view with backup
			vs.toNewView(vs.view.Backup, "")
			// shouldn't find backup from pool this time since the new primary hasn't acked
		}

	}

	if vs.view.Backup != "" {
		backupTime := vs.clients[vs.view.Backup]
		if backupTime.Add(PingInterval * DeadPings).Before(time.Now()) {
			// mark as dead
			vs.view.Backup = ""
		}
	} else {
		// backup empty
		// check pool
		if vs.pool.Len() > 0 && vs.acked {
			// find new backup from pool
			vs.toNewView(vs.view.Primary, vs.tryFetchFromPool())
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	// added init
	vs.view = View{0, "", ""}
	vs.acked = false
	vs.primaryPinged = time.Now()
	vs.backupPinged = time.Now()
	vs.pool = list.New()
	vs.clients = make(map[string]time.Time)
	// end of added init

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
