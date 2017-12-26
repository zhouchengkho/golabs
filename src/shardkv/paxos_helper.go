package shardkv

import (
	"paxos"
	"time"
	"crypto/rand"
	"math/big"
)


func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func genOp(name string, args Args) (Op) {
	return Op{Id: nrand(),
		Name: name,
		Args: args,
	}
}


func (kv *ShardKV) agree(operation Op) (int) {
	var seq int
	var decidedOp = Op{}

	for decidedOp.Id != operation.Id {
		seq = kv.px.Max() + 1
		kv.px.Start(seq, operation)
		decidedOp = kv.getDecidedVal(seq).(Op)  // type assertion
	}
	return seq
}

func (kv *ShardKV) getDecidedVal(seq int) (interface{}) {
	sleepTime := SLEEPTIME
	for {
		status, val := kv.px.Status(seq)
		if status == paxos.Decided {
			return val
		}
		time.Sleep(sleepTime)
		if sleepTime < SLEEPMAX {
			sleepTime *= 2
		}
	}
}

func (kv *ShardKV) proposeNoOp() {
	NoOp := genOp(NoOp, NoOpArgs{})
	seq := kv.agree(NoOp)

	kv.catchup(seq)
	kv.doOp(seq, NoOp)
}



func (kv *ShardKV) catchup(seq int) {
	lastSeq := kv.lastSeq + 1
	status, operation := kv.px.Status(lastSeq)

	for lastSeq < seq {

		if status == paxos.Decided {
			kv.doOp(lastSeq, operation.(Op))
			lastSeq = kv.lastSeq + 1
			status, operation = kv.px.Status(lastSeq)
		} else {
			NoOp := genOp(NoOp, NoOpArgs{})
			kv.px.Start(lastSeq, NoOp)
			kv.getDecidedVal(lastSeq)

			status, operation = kv.px.Status(lastSeq)
			kv.doOp(lastSeq, operation.(Op))
			lastSeq = kv.lastSeq + 1
			status, operation = kv.px.Status(lastSeq)
		}
	}
}

func (kv *ShardKV) doOp(seq int, operation Op) OpResult {
	var result OpResult

	switch operation.Name {
	case Get:
		var getArgs = (operation.Args).(GetArgs)
		result = kv.get(&getArgs)
		break
	case Put:
		var putAppendArgs = (operation.Args).(PutAppendArgs)
		result = kv.putAppend(&putAppendArgs)
		break
	case Append:
		var putAppendArgs = (operation.Args).(PutAppendArgs)
		result = kv.putAppend(&putAppendArgs)
		break
	case ReceiveShard:
		var receiveShardArgs = (operation.Args).(ReceiveShardArgs)
		result = kv.receiveShard(&receiveShardArgs)
		break
	case SentShard:
		var sentShardArgs = (operation.Args).(SentShardArgs)
		result = kv.sentShard(&sentShardArgs)
		break
	case StartNewConfig:
		var reConfigStartArgs = (operation.Args).(ReConfigStartArgs)
		result = kv.startNewConfig(&reConfigStartArgs)
		break
	case EndNewConfig:
		var reConfigEndArgs = (operation.Args).(ReConfigEndArgs)
		result = kv.endNewConfig(&reConfigEndArgs)
		break
	case NoOp:
		break
	default:
		break
	}
	kv.lastSeq = seq
	kv.px.Done(seq)
	return result
}
