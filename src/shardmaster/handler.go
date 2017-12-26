package shardmaster

import (
	"paxos"
	"time"
)

func (sm *ShardMaster) joinHandler(args *JoinArgs) {
	op := genOp(Join, *args)
	seq := sm.agree(op)
	sm.catchup(seq)
	sm.doOp(seq, op)
}

func (sm *ShardMaster) leaveHandler(args *LeaveArgs) {
	op := genOp(Leave, *args)
	seq := sm.agree(op)
	sm.catchup(seq)
	sm.doOp(seq, op)

}

func (sm *ShardMaster) moveHandler(args *MoveArgs) {
	op := genOp(Move, *args)
	seq := sm.agree(op)
	sm.catchup(seq)
	sm.doOp(seq, op)
}

func (sm *ShardMaster) queryHandler(args *QueryArgs) {

	op := genOp(Query, *args)

	seq := sm.agree(op)
	sm.catchup(seq)
	sm.doOp(seq, op)

}


func (sm *ShardMaster) groupJoin(gid int64, servers []string)  {
	configs := sm.configs
	newConfig := createConfigs(configs)

	prior_config := sm.configs[len(sm.configs)-1]   // previous Config in ShardMaster.configs
	config := prior_config.copy()                       // newly created Config

	config.addGroup(gid, servers)
	config.occupyInvalid()
	config.checkLoad(1)
	newConfig[len(newConfig)-1] = config
	sm.configs = newConfig
}

func (sm *ShardMaster) groupLeave(gid int64) {

	configs := sm.configs
	newConfig := createConfigs(configs)

	prior_config := sm.configs[len(sm.configs)-1]   // previous Config in ShardMaster.configs
	config := prior_config.copy()                       // newly created Config

	config.removeGroupByGID(gid)
	config.moveAround(gid)
	config.checkLoad(1)
	newConfig[len(newConfig)-1] = config
	sm.configs = newConfig
}

func (sm *ShardMaster) groupMove(num int, gid int64) {
	configs := sm.configs
	newConfig := createConfigs(configs)
	groups := groupAfterMove(configs)
	shards := shardsAfterMove(configs, num, gid)
	newConfig[len(newConfig)-1] = Config{len(newConfig)-1, shards, groups}
	sm.configs =  newConfig
}

func (sm *ShardMaster) groupQuery(num int) Config {
	configs := sm.configs
	if num < 0 || num >= len(configs) {
		return configs[len(configs) - 1]
	} else {
		return configs[num]
	}
}

func (sm *ShardMaster) doOp(seq int, operation Op) {
	switch operation.Type {
	case Join:
		sm.groupJoin(operation.Args.(JoinArgs).GID, operation.Args.(JoinArgs).Servers)
		break
	case Leave:
		sm.groupLeave(operation.Args.(LeaveArgs).GID)
		break
	case Move:
		sm.groupMove(operation.Args.(MoveArgs).Shard, operation.Args.(MoveArgs).GID)
		break
	case Query:
		sm.groupQuery(operation.Args.(QueryArgs).Num)
		break
	default:
		break
	}
	sm.lastSeq = seq
	sm.px.Done(seq)
}

func (sm *ShardMaster) agree(operation Op) int {
	var seq int // paxos seq
	decidedOp := Op{}
	for decidedOp.Uid != operation.Uid {
		seq = sm.px.Max() + 1
		sm.px.Start(seq, operation)
		decidedOp = sm.getDecidedVal(seq).(Op)
	}
	return seq
}

func (sm *ShardMaster) getDecidedVal(seq int) (val interface{}){
	sleep := SLEEPTIME
	for {
		status, val := sm.px.Status(seq)
		if status == paxos.Decided {
			return val
		}
		time.Sleep(sleep)
		if sleep < MAXSLEEP {
			sleep *= 2
		}
	}
}

func (sm *ShardMaster) catchup(seq int) {
	lastSeq := sm.lastSeq + 1
	status, op := sm.px.Status(lastSeq)

	for lastSeq < seq {
		if status == paxos.Decided {
			sm.doOp(lastSeq, op.(Op))
			lastSeq = sm.lastSeq + 1
			status, op = sm.px.Status(lastSeq)
		} else {
			noop := genOp(NoOp, JoinArgs{}) // doesn't matter what args put in
			sm.px.Start(lastSeq, noop)
			sm.getDecidedVal(lastSeq)
			lastSeq = sm.lastSeq + 1
			status, op = sm.px.Status(lastSeq)
		}
	}

}
