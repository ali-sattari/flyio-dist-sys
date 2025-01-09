package kafka

import (
	"slices"
	"sync"
)

type msgLog struct {
	offset int64
	msg    int64
}

type keyStore struct {
	key       string
	committed int64
	mtx       *sync.RWMutex
	offsets   []int64
}

func NewKeyStore(key string) *keyStore {
	return &keyStore{
		key:       key,
		committed: 0,
		mtx:       &sync.RWMutex{},
		offsets:   []int64{},
	}
}

func (k *keyStore) store(offset int64) {
	k.mtx.Lock()
	defer k.mtx.Unlock()

	k.offsets = append(k.offsets, offset)
	slices.Sort(k.offsets)
}

func (k *keyStore) getOffsets(offset int64) []int64 {
	k.mtx.RLock()
	defer k.mtx.RUnlock()

	res := []int64{}
	if offset > 0 && contains(k.offsets, offset) == false {
		return res
	}

	curr := offset
	for _, l := range k.offsets {
		if l >= offset && l-curr <= 1 {
			res = append(res, l)
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
