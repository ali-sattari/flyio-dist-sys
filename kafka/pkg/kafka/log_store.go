package kafka

import (
	"fmt"
)

type msgLog struct {
	offset int64
	msg    int64
}

type keyStore struct {
	key       string
	committed int64
	logs      []msgLog
}

func (k *keyStore) write(msg, offset int64) msgLog {
	l := msgLog{
		offset: offset,
		msg:    msg,
	}
	k.logs = append(k.logs, l)
	return l
}

func (k *keyStore) read(offset int64) []msgLog {
	res := []msgLog{}
	for _, l := range k.logs {
		if l.offset >= offset {
			res = append(res, l)
		}
	}
	return res
}

func (k *keyStore) commitOffset(offset int64) error {
	if offset < k.committed {
		return fmt.Errorf("offset %d is older than already committed offset %d", offset, k.committed)
	}
	k.committed = offset
	return nil
}

func (k *keyStore) getCommittedOffset() int64 {
	return k.committed
}
