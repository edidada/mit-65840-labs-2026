package rpc

// Local clone of the kvsrv1 rpc wire types, extended with the clerk
// (ClientID, Seq) pair that Lab 4 needs for at-most-once execution.
type Err string

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrVersion     = "ErrVersion"
	ErrMaybe       = "ErrMaybe"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	Key      string
	Value    string
	Version  Tversion
	ClientID int64
	Seq      int64
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientID int64
	Seq      int64
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

// TPutArgs is the kvraft wire form that carries the clerk identity.
type TPutArgs struct {
	PutArgs
	ClientID int64
	Seq      int64
}

type TGetArgs struct {
	GetArgs
	ClientID int64
	Seq      int64
}
