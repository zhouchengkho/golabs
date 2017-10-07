package mapreduce

import (
	"container/list"
	"fmt"
	// "strconv"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
	// ADDED CODE
	busy bool
	out  bool
	// END OF ADDED CODE
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func getDoJobArg(file string, op JobType, no int, otherPhase int) (*DoJobArgs, *DoJobReply) {
	args := &DoJobArgs{}
	reply := &DoJobReply{}
	args.File = file
	args.Operation = op
	args.JobNumber = no
	args.NumOtherPhase = otherPhase
	return args, reply
}

func (mr *MapReduce) fetchNewWorker() {
	for true {
		w := &WorkerInfo{}
		w.address = <-mr.registerChannel
		w.busy = false
		w.out = false
		mr.Workers[w.address] = w
		fmt.Printf("new worker in line: %s \n", w.address)
		mr.nextWorkerAddr <- w.address
	}
}

func (mr *MapReduce) BusyJobThenFree(args *DoJobArgs, reply *DoJobReply) bool {
	thisWorker := <-mr.nextWorkerAddr
	ok := call(thisWorker, "Worker.DoJob", args, reply)
	for !ok {
		fmt.Printf("oops worker %s failed\n", thisWorker)
		mr.Workers[thisWorker].out = true
		thisWorker = <-mr.nextWorkerAddr
		ok = call(thisWorker, "Worker.DoJob", args, reply)

	}
	go func() {
		mr.nextWorkerAddr <- thisWorker
	}()
	return ok
}
func doThisJob(mr *MapReduce, t JobType, number int, count *int, done chan bool) {
	// get job count and other count
	jobCount := mr.nMap
	other := mr.nReduce
	if t == Reduce {
		jobCount = mr.nReduce
		other = mr.nMap
	}
	args, reply := getDoJobArg(mr.file, t, number, other)
	// thisWorker := <-mr.nextWorkerAddr
	mr.BusyJobThenFree(args, reply)

	*count = *count + 1
	if *count == jobCount {
		done <- true
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	// set up a goroutine, keep fetching new worker into map
	go mr.fetchNewWorker()

	// set task done count, after mapDone, proceed to reduce
	//  after reduce done, proceed to kill Workers
	mapCount := 0
	reduceCount := 0
	mapDone := make(chan bool)
	reduceDone := make(chan bool)

	// do map job
	for i := 0; i < mr.nMap; i++ {
		go doThisJob(mr, Map, i, &mapCount, mapDone)
	}
	// after map done, do reduce
	<-mapDone

	// do reduce job
	for i := 0; i < mr.nReduce; i++ {
		go doThisJob(mr, Reduce, i, &reduceCount, reduceDone)
	}

	// wait until reduce done
	<-reduceDone

	return mr.KillWorkers()
}
