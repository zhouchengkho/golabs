package shardkv


func (kv *ShardKV) getHandler(args *GetArgs) GetReply {
	if kv.currentConfig.Num == 0 {
		return GetReply{}
	}

	operation := genOp(Get, *args)
	seq := kv.agree(operation)

	kv.catchup(seq)
	return kv.doOp(seq, operation).(GetReply)
}

func (kv *ShardKV) putAppendHandler(args *PutAppendArgs) PutAppendReply {
	if kv.currentConfig.Num == 0 {
		return PutAppendReply{}
	}

	operation := genOp(args.Op, *args)
	seq := kv.agree(operation)

	kv.catchup(seq)
	return kv.doOp(seq, operation).(PutAppendReply)
}

func (kv *ShardKV) receiveShardHandler(args *ReceiveShardArgs) ReceiveShardReply {
	if kv.currentConfig.Num == 0 {
		return ReceiveShardReply{}
	}

	operation := genOp(ReceiveShard, *args)
	seq := kv.agree(operation)

	kv.catchup(seq)
	return kv.doOp(seq, operation).(ReceiveShardReply)
}