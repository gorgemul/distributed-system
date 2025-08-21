package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type state struct {
	key     string
	version rpc.Tversion
}

type Lock struct {
	ck kvtest.IKVClerk
	id string
	state
}

func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	return &Lock{ck, kvtest.RandValue(8), state{ key: l }}
}

func (lk *Lock) Acquire() {
	for {
		id, version, err := lk.ck.Get(lk.state.key)
		if err == rpc.ErrNoKey || id == "" || id == lk.id {
			if err := lk.ck.Put(lk.state.key, lk.id, version); err == rpc.OK {
				lk.state.version = version + 1
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	lk.ck.Put(lk.state.key, "", lk.state.version)
}
