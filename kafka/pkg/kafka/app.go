package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node        NodeInterface
	storage     map[string]*keyStore
	storageMtx  *sync.RWMutex
	storageChan chan map[string]MsgLog

	linKv KVInterface
	seqKv KVInterface

	topo      []string
	gossiped  map[string][]gossipLog
	ackMtx    *sync.Mutex
	buffer    map[string]msg_log_list
	bufMtx    *sync.Mutex
	lastFlush time.Time
	flushMtx  *sync.Mutex
}

type workload struct {
	maelstrom.MessageBody
	Type           string           `json:"type"`
	Key            string           `json:"key,omitempty"`
	Keys           []string         `json:"keys,omitempty"`
	Msg            int64            `json:"msg,omitempty"`
	Offset         int64            `json:"offset,omitempty"`
	Offsets        map[string]int64 `json:"offsets,omitempty"`
	GossipId       string           `json:"gossip_id,omitempty"`
	GossipMessages []MsgLog         `json:"gossip_messages,omitempty"`
}

type offset_msg_pair [2]int64 //offset,msg
type key_offset_pair map[string]int64
type message_list map[string][]offset_msg_pair
type offset_list map[string][]int64   //key:[]int64
type msg_log_list map[string][]MsgLog //key:[]msglog

type baseResponse struct {
	Type string `json:"type"`
}
type sendResponse struct {
	Type   string `json:"type"`
	Offset int64  `json:"offset"`
}
type pollResponse struct {
	Type string       `json:"type"`
	Msgs message_list `json:"msgs"`
}
type listCommittedOffsetsResponse struct {
	Type    string          `json:"type"`
	Offsets key_offset_pair `json:"offsets"`
}

var wg sync.WaitGroup

const (
	LAST_OFFSET_START    = 0
	MAX_POLL_LIST_LENGTH = 10
)

func New(n NodeInterface, linKv, seqKv KVInterface) Program {
	p := Program{
		node:        n,
		linKv:       linKv,
		seqKv:       seqKv,
		storage:     map[string]*keyStore{},
		storageMtx:  &sync.RWMutex{},
		storageChan: make(chan map[string]MsgLog, 5),
		topo:        []string{},
		gossiped:    map[string][]gossipLog{},
		ackMtx:      &sync.Mutex{},
		buffer:      map[string]msg_log_list{},
		bufMtx:      &sync.Mutex{},
		flushMtx:    &sync.Mutex{},
	}

	wg.Add(1)
	go p.storeMsgWorker()

	retrier := p.periodicJobs(1*time.Second, p.resendUnackedGossips)
	flusher := p.periodicJobs(1+bufferAge, p.flushGossipBuffer)

	wg.Add(2)
	go retrier()
	go flusher()

	return p
}

func (p *Program) GetHandle(rpc_type string) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body workload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var resp any
		switch rpc_type {
		case "init":
			resp = p.handleInit()
		case "send":
			resp = p.handleSend(body)
			p.gossip(msg.Src, body.Key, MsgLog{Offset: resp.(sendResponse).Offset, Msg: body.Msg})
		case "poll":
			resp = p.handlePoll(body)
		case "commit_offsets":
			resp = p.handleCommitOffsets(body)
		case "list_committed_offsets":
			resp = p.handleListCommittedOffsets(body)
		case "gossip":
			resp = p.receiveGossip(body, msg)
		case "gossip_ok":
			p.ackGossip(body, msg)
			return nil

		}

		return p.node.Reply(msg, resp)
	}
}

func (p *Program) handleInit() baseResponse {
	for _, n := range p.node.NodeIDs() {
		if n != p.node.ID() {
			p.topo = append(p.topo, n)
		}
	}
	// log.Printf("handleInit: topology for %s updated to: %+v", p.node.ID(), p.topo)
	return baseResponse{Type: "init_ok"}
}

func (p *Program) handleSend(body workload) sendResponse {
	o := p.getNextOffset(body.Key)

	p.storageChan <- map[string]MsgLog{
		body.Key: {Offset: o, Msg: body.Msg},
	}

	resp := sendResponse{
		Type:   "send_ok",
		Offset: o,
	}
	return resp
}

func (p *Program) storeMsg(key string, entry MsgLog) {
	ks := p.getKeyStore(key)
	ks.store(entry)
}

func (p *Program) getNextOffset(key string) int64 {
	offset_key := formatLastOffsetKey(key)
	for {
		current, _ := p.linKv.ReadInt(context.Background(), offset_key)
		next := current + 1
		err := p.linKv.CompareAndSwap(context.Background(), offset_key, current, next, true)
		if err != nil {
			switch e := err.(type) {
			case *maelstrom.RPCError:
				if e.Code == maelstrom.PreconditionFailed {
					log.Printf("getNextOffset: CAS PreconditionFailed for %d -> %d", current, next)
					continue
				}
			default:
				log.Printf("getNextOffset: for %s error %d -> %d: %s", key, current, next, err)
			}
		}
		return int64(next)
	}
}

func (p *Program) handlePoll(body workload) pollResponse {
	logs := message_list{}
	for k, o := range body.Offsets {
		ks := p.getKeyStore(k)
		logs[k] = []offset_msg_pair{}
		count := 0
		// log.Printf("handlePoll key offsets: %+v", ol)
		for _, m := range ks.getMessages(o) {
			if count == MAX_POLL_LIST_LENGTH {
				break
			}
			// val, err := p.seqKv.ReadInt(context.Background(), formatMsgKey(k, msg.offset))
			// log.Printf("handlePoll: ReaInt for %s:%d -> %+v", k, offset, val)
			// if err != nil {
			// 	switch e := err.(type) {
			// 	case *maelstrom.RPCError:
			// 		if e.Code == maelstrom.KeyDoesNotExist {
			// 			log.Printf("handlePoll: KeyDoesNotExist on ReaInt for %s:%d", k, msg.offset)
			// 			continue
			// 		}
			// 	}
			// }
			logs[k] = append(logs[k], [2]int64{m.Offset, m.Msg})
			count++
		}

	}

	res := pollResponse{
		Type: "poll_ok",
		Msgs: logs,
	}
	return res
}

func (p *Program) handleCommitOffsets(body workload) baseResponse {
	resp := baseResponse{Type: "commit_offsets_ok"}
	for k, o := range body.Offsets {
		ks := p.getKeyStore(k)
		curr := ks.getCommittedOffset()
		ks.commitOffset(o)

		if curr == o {
			// skip linkv cas
			return resp
		}

		_ = p.linKv.CompareAndSwap(context.Background(), formatCommittedOffsetKey(k), curr, o, true)
	}
	return resp
}

func (p *Program) handleListCommittedOffsets(body workload) listCommittedOffsetsResponse {
	res := listCommittedOffsetsResponse{
		Type:    "list_committed_offsets_ok",
		Offsets: key_offset_pair{},
	}
	for _, k := range body.Keys {
		kv, err := p.linKv.ReadInt(context.Background(), formatCommittedOffsetKey(k))
		if err != nil {
			log.Printf("handleListCommittedOffsets: error ReadInt on %s: %+v", k, err)
			res.Offsets[k] = 0
		} else {
			res.Offsets[k] = int64(kv)
			p.getKeyStore(k).commitOffset(int64(kv))
		}
	}
	return res
}

func (p *Program) storeMsgWorker() {
	for msgs := range p.storageChan {
		for key, logEntry := range msgs {
			p.storeMsg(key, logEntry)
		}
	}
}

func (p *Program) getKeyStore(key string) *keyStore {
	p.storageMtx.Lock()
	defer p.storageMtx.Unlock()

	if ks, ok := p.storage[key]; ok {
		return ks
	}
	ks := NewKeyStore(key)
	p.storage[key] = ks
	return ks
}

func (p *Program) Shutdown() {
	close(p.storageChan)
	wg.Wait()
}

func (p *Program) periodicJobs(dur time.Duration, f func()) func() {
	ticker := time.NewTicker(dur)
	return func() {
		for {
			select {
			case <-ticker.C:
				f()
			}
		}
	}
}
