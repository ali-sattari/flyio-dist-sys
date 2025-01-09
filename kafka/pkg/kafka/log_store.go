package kafka

import (
	"maps"
	"slices"
	"sync"
)

type MsgLog struct {
	Offset int64 `json:"offset"`
	Msg    int64 `json:"msg"`
}

type keyStore struct {
	key       string
	committed int64
	mtx       *sync.RWMutex
	offsets   []int64
	messages  map[int64]int64
}

func NewKeyStore(key string) *keyStore {
	return &keyStore{
		key:       key,
		committed: 0,
		mtx:       &sync.RWMutex{},
		offsets:   []int64{},
		messages:  map[int64]int64{},
	}
}

func (k *keyStore) store(msg MsgLog) {
	k.mtx.Lock()
	defer k.mtx.Unlock()

	k.messages[msg.Offset] = msg.Msg
	k.offsets = slices.Sorted(maps.Keys(k.messages))
}

func (k *keyStore) getMessages(offset int64) []MsgLog {
	k.mtx.RLock()
	defer k.mtx.RUnlock()

	res := []MsgLog{}
	if offset > 0 && contains(k.offsets, offset) == false {
		return res
	}

	curr := offset
	for _, l := range k.offsets {
		if l >= offset && l-curr <= 1 {
			res = append(res, MsgLog{Offset: l, Msg: k.messages[l]})
			curr = l
		}
	}
	return res
}

func (k *keyStore) commitOffset(offset int64) {
	k.mtx.Lock()
	defer k.mtx.Unlock()

	k.committed = offset
}

func (k *keyStore) getCommittedOffset() int64 {
	k.mtx.RLock()
	defer k.mtx.RUnlock()

	return k.committed
}
