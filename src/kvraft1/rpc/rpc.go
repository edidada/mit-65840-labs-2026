package rpc

// Kvraft wire types. They carry the clerk identity (Id, Seq) on every
// call so the rsm duplicate table can decide at-most-once replay.

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrVersion     Err = "ErrVersion"
	ErrMaybe       Err = "ErrMaybe"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrWrongGroup  Err = "ErrWrongGroup"
)

type Tversion uint64

// TGetArgs is the wire form of Get, with clerk identity.
type TGetArgs struct {
	Key string
	Id  int64
	Seq int64
}

type TGetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

// TPutArgs is the wire form of Put, with clerk identity.
type TPutArgs struct {
	Key     string
	Value   string
	Version Tversion
	Id      int64
	Seq     int64
}

type TPutReply struct {
	Err Err
}
