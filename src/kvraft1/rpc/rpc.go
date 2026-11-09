package rpc

import (
	kvsrv "6.5840/kvsrv1/rpc"
)

// Err and Tversion are shared with the kvsrv1 wire package so typed
// values flow without conversions between clerk and server.
type Err = kvsrv.Err
type Tversion = kvsrv.Tversion

const (
	OK             = kvsrv.OK
	ErrNoKey       = kvsrv.ErrNoKey
	ErrVersion     = kvsrv.ErrVersion
	ErrMaybe       = kvsrv.ErrMaybe
	ErrWrongLeader = kvsrv.ErrWrongLeader
	ErrWrongGroup  = kvsrv.ErrWrongGroup
)

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
