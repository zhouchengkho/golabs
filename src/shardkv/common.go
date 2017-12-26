package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	NotReady = "NotReady"
)

const (
	Get = "Get"
	Put = "Put"
	Append = "Append"
	StartNewConfig = "StartNewConfig"
	EndNewConfig = "EndNewConfig"
	SentShard = "SentShard"
	ReceiveShard = "ReceiveShard"
	NoOp = "NoOp"
)

const (
	SLEEPTIME = 10 * time.Millisecond
	SLEEPMAX = 5 * time.Second
)

type Err string

type Args interface{}
type OpResult interface{}
type Reply interface{}

type ReConfigStartArgs struct {}
type ReConfigEndArgs struct {}
type NoOpArgs struct {}

type KVPair struct {
	Key string
	Value string
}

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int
	RequestId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int
	RequestId int
}

type GetReply struct {
	Err   Err
	Value string
}

type ReceiveShardArgs struct {
	Kvpairs []KVPair
	NextTransition int
	Index int
}

type ReceiveShardReply struct {
	Err Err
}

type SentShardArgs struct {
	NextTransition int
	Index int
}


