package pbservice

const (
	OK                     = "OK"
	ErrNoKey               = "ErrNoKey"
	ErrWrongServer         = "ErrWrongServer"
	ErrAlreadyTransmitted  = "ErrAlreadyTransmitted"
	Test                   = "Test"
	Init                   = "Init"
	ErrBackupForwardFailed = "ErrBackupForwardFailed"
	ErrPossibleStale       = "ErrPossibleStale"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op   string
	Num  int64
	From string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// basically add sequence number to ensure transmission is dealt once
	Num int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type InitArgs struct {
	Map  map[string]string
	Nums map[int64]bool
}

type InitReply struct {
	Err Err
}

type ApplyArgs struct {
	Ele PutAppendArgs
}

type ApplyReply struct {
	Err Err
}

type ForwardArgs struct {
	Args PutAppendArgs
}

type ForwardReply struct {
	Err Err
	val string
}

type CheckArgs struct {
	ViewNum uint
}

type CheckReply struct {
	Err Err
}
