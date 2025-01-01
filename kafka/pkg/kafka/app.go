package kafka

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node          *maelstrom.Node
	storage       map[string]*keyStore
	storageMtx    *sync.RWMutex
	storageChan   chan map[string]msgLog
	lastOffset    int64
	lastOffsetMtx *sync.Mutex
}

type workload struct {
	maelstrom.MessageBody
	Type    string
	Key     string
	Keys    []string
	Msg     int64
	Offset  int64
	Offsets map[string]int64
}

type message_list map[string][][2]int64
type offset_list map[string]int64

type response struct {
	Type    string       `json:"type,omitempty"`
	Msgs    message_list `json:"msgs,omitempty"`
	Offset  int64        `json:"offset,omitempty"`
	Offsets offset_list  `json:"offsets,omitempty"`
}

var wg sync.WaitGroup

const LAST_OFFSET_START = 0

func New(n *maelstrom.Node) Program {
	p := Program{
		node:          n,
		storage:       map[string]*keyStore{},
		storageMtx:    &sync.RWMutex{},
		storageChan:   make(chan map[string]msgLog, 5),
		lastOffset:    LAST_OFFSET_START,
		lastOffsetMtx: &sync.Mutex{},
	}

	wg.Add(1)
	go p.storeMsgWorker()

	return p
}

func (p *Program) GetHandle(rpc_type string) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body workload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var resp response
		switch rpc_type {
		case "send":
			resp = p.handleSend(body)
		case "poll":
			resp = p.handlePoll(body)
		case "commit_offsets":
			resp = p.handleCommitOffsets(body)
		case "list_committed_offsets":
			resp = p.handleListCommittedOffsets(body)
		}

		return p.node.Reply(msg, resp)
	}
}

func (p *Program) handleSend(body workload) response {
	o := p.getNewOffset()

	p.storageChan <- map[string]msgLog{
		body.Key: {offset: o, msg: body.Msg},
	}

	resp := response{
		Type:   "send_ok",
		Offset: o,
	}
	return resp
}

func (p *Program) handlePoll(body workload) response {
	p.storageMtx.RLock()
	defer p.storageMtx.RUnlock()

	logs := message_list{}
	for k, o := range body.Offsets {
		key, ok := p.storage[k]
		if ok {
			for _, ml := range key.read(o) {
				logs[k] = append(logs[k], [2]int64{ml.offset, ml.msg})
			}
		} else {
			logs[k] = nil
		}
		// log.Printf("offsets for key %s at %d: %+v\n", k, o, logs[k])
	}

	res := response{
		Type: "poll_ok",
		Msgs: logs,
	}
	return res
}

func (p *Program) handleCommitOffsets(body workload) response {
	p.storageMtx.RLock()
	defer p.storageMtx.RUnlock()

	for k, o := range body.Offsets {
		if key, ok := p.storage[k]; ok {
			err := key.commitOffset(o)
			if err != nil {
				log.Printf("error setting commit offset for key %+v: %s", key, err)
			}
		}
	}
	return response{Type: "commit_offsets_ok"}
}

func (p *Program) handleListCommittedOffsets(body workload) response {
	p.storageMtx.RLock()
	defer p.storageMtx.RUnlock()

	res := response{
		Type:    "list_committed_offsets_ok",
		Offsets: offset_list{},
	}
	for _, k := range body.Keys {
		if key, ok := p.storage[k]; ok {
			res.Offsets[k] = key.getCommittedOffset()
		} else {
			res.Offsets[k] = 0
		}
	}
	return res
}

func (p *Program) getNewOffset() int64 {
	p.lastOffsetMtx.Lock()
	defer p.lastOffsetMtx.Unlock()

	p.lastOffset += 1
	return p.lastOffset
}

func (p *Program) storeMsgWorker() {
	for msgs := range p.storageChan {
		for key, logEntry := range msgs {
			p.storeMsg(key, logEntry)
		}
	}
}

func (p *Program) storeMsg(key string, entry msgLog) {
	p.storageMtx.Lock()
	defer p.storageMtx.Unlock()

	_, ok := p.storage[key]
	if !ok {
		ks := keyStore{
			key:       key,
			committed: 0,
			logs:      []msgLog{},
		}
		p.storage[key] = &ks
	}
	p.storage[key].write(entry.msg, entry.offset)
	// log.Printf("total %d keys in storage, stored msg %+v (%+v)\n", len(p.storage), res, p.storage)
}

func (p *Program) Shutdown() {
	close(p.storageChan)
	wg.Wait()
}
