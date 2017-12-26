package shardkv

import (
	"strconv"
	"shardmaster"
)

func (kv *ShardKV) get(args *GetArgs) OpResult {
	uid := convRequest(args.ClientId, args.RequestId)

	// disable cache
	r, exist := kv.cache[uid]
	if exist {
		return r
	}

	reply := GetReply{}

	if kv.ownsShard(args.Key) {
		if kv.hasShard(args.Key) {
			value, present := kv.data[args.Key]
			if present {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Value = ""
				reply.Err = ErrNoKey
			}
			kv.cache[uid] = reply
			return reply
		}
		reply.Err = NotReady
		return reply
	}
	reply.Err = ErrWrongGroup
	return reply
}

func (kv *ShardKV) putAppend(args *PutAppendArgs) OpResult {
	uid := convRequest(args.ClientId, args.RequestId)

	r, exist := kv.cache[uid]
	if exist {
		return r
	}

	reply := PutAppendReply{}

	if kv.ownsShard(args.Key) {
		if kv.hasShard(args.Key) {
			if args.Op == Put {
				kv.data[args.Key] = args.Value
			} else if args.Op == Append {
				_, exist := kv.data[args.Key]
				if exist {
					kv.data[args.Key] = kv.data[args.Key] + args.Value
				} else {
					kv.data[args.Key] = args.Value
				}
			}
			putReply := PutAppendReply{Err: OK}

			kv.cache[uid] = putReply
			return putReply
		}
		reply.Err = NotReady
		return reply
	}

	reply.Err = ErrWrongGroup
	return reply
}

func (kv *ShardKV) ownsShard(key string) bool {
	i := key2shard(key)
	return kv.currentConfig.Shards[i] == kv.gid
}

func (kv *ShardKV) hasShard(key string) bool {
	i := key2shard(key)
	return kv.shards[i]
}

func (kv *ShardKV) sendOk() bool {
	target := getShardState(kv.currentConfig.Shards, kv.gid)
	for Index, _ := range kv.shards {
		if kv.shards[Index] == true && target[Index] == false {
			// still at least one send has not been acked
			return false
		}
	}
	return true
}

func (kv *ShardKV) receiveOk() bool {
	target := getShardState(kv.currentConfig.Shards, kv.gid)
	for Index, _ := range kv.shards {
		if kv.shards[Index] == false && target[Index] == true {
			return false
		}
	}
	return true
}

func (kv *ShardKV) sendShardStateToPeer() {
	target := getShardState(kv.currentConfig.Shards, kv.gid)
	for Index, _ := range kv.shards {
		if kv.shards[Index] == true && target[Index] == false {
			gid := kv.currentConfig.Shards[Index]
			kv.sendShard(Index, gid)
		}
	}
	return
}

func (kv *ShardKV) sentShard(args *SentShardArgs) OpResult {
	if kv.nextTransitionNumber == args.NextTransition {
		i := args.Index
		for key := range kv.data {
			if key2shard(key) == i {
				delete(kv.data, key)
			}
		}
		kv.shards[i] = false
	}
	return nil
}

func (kv *ShardKV) sendShard(Index int, gid int64) {
	var kvpairs []KVPair
	for key,value := range kv.data {
		if key2shard(key) == Index {
			kvpairs = append(kvpairs, KVPair{Key: key, Value: value})
		}
	}

	servers := kv.currentConfig.Groups[gid]

	for _, srv := range servers {
		args := &ReceiveShardArgs{}   
		args.Kvpairs = kvpairs
		args.NextTransition = kv.nextTransitionNumber
		args.Index = Index
		var reply ReceiveShardReply
		ok := call(srv, "ShardKV.ReceiveShard", args, &reply)
		if ok && reply.Err == OK {
			sentShardArgs := SentShardArgs{Index: Index, NextTransition: kv.nextTransitionNumber}
			operation := genOp(SentShard, sentShardArgs)
			seq := kv.agree(operation)
			kv.catchup(seq)
			kv.doOp(seq, operation)
			return
		}
	}
}

func (kv *ShardKV) receiveShard(args *ReceiveShardArgs) OpResult {
	uid := convUID(args.NextTransition, args.Index) // string

	r, present := kv.cache[uid]
	if present {
		return r
	}

	reply := ReceiveShardReply{}

	if kv.nextTransitionNumber < args.NextTransition {
		reply.Err = NotReady
		return reply

	} else if kv.nextTransitionNumber == args.NextTransition {
		for _, pair := range args.Kvpairs {
			kv.data[pair.Key] = pair.Value
		}
		kv.shards[args.Index] = true
		reply.Err = OK
		kv.cache[uid] = reply
		return reply
	}
	reply.Err = OK
	kv.cache[uid] = reply
	return reply
}

func (kv *ShardKV) startNewConfig(args *ReConfigStartArgs) OpResult {

	if kv.nextTransitionNumber == kv.currentConfig.Num {
		return nil
	}
	nextConfig := kv.sm.Query(kv.currentConfig.Num + 1)    // next Config
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = nextConfig
	kv.nextTransitionNumber = kv.currentConfig.Num

	kv.shards = getShardState(kv.lastConfig.Shards, kv.gid)

	return nil
}

func (kv *ShardKV) endNewConfig(args *ReConfigEndArgs) OpResult {
	if kv.nextTransitionNumber == -1 {
		return nil
	}
	kv.nextTransitionNumber = -1
	return nil
}




func getShardState(shards [shardmaster.NShards]int64, targetGID int64) []bool {
	state := make([]bool, len(shards))
	for Index, gid := range shards {
		if gid == targetGID {
			state[Index] = true
		} else {
			state[Index] = false
		}
	}
	return state
}


func convRequest(clientId int, requestId int) string {
	return strconv.Itoa(clientId) + ":" + strconv.Itoa(requestId)
}

func convUID(clientId int, requestId int) string {
	return "i" + strconv.Itoa(clientId) + ":" + strconv.Itoa(requestId)
}
