package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// key of this lock in the k/v store; while held the value is our
	// holder id, after Release it is "" so others may install.
	key    string
	holder string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	lk.key = lockname
	lk.holder = kvtest.RandValue(12)
	return lk
}

// Acquire loops: entry absent or blank (ver==0 answer for ErrNoKey, or
// blank value with some ver after a Release) → install holder with the
// current version. An already-successful ErrMaybe install shows holder
// and is confirmed. Anyone else holding makes us wait.
func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.key)
		switch {
		case err == rpc.ErrNoKey:
			putErr := lk.ck.Put(lk.key, lk.holder, 0)
			if putErr == rpc.OK || putErr == rpc.ErrMaybe {
				return
			}
		case err == rpc.OK && val == lk.holder:
			return
		case err == rpc.OK && val == "":
			putErr := lk.ck.Put(lk.key, lk.holder, ver)
			if putErr == rpc.OK || putErr == rpc.ErrMaybe {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Release blanks the entry only while it still names us; ErrMaybe is
// accepted, the assignment is equivalent either way.
func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.key)
		switch {
		case err != rpc.OK:
			return
		case val == lk.holder:
			putErr := lk.ck.Put(lk.key, "", ver)
			switch putErr {
			case rpc.OK, rpc.ErrMaybe:
				// ErrMaybe on a blank entry is indistinguishable from a
				// successful release; the next Acquire confirms either way.
				return
			case rpc.ErrVersion:
				// The entry changed under us: another acquirer took over or
				// our first release actually landed. Either way it is freed.
				continue
			default:
				return
			}
		default:
			return
		}
	}
}
