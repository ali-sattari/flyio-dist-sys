package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node        NodeInterface
	storage     map[string]*keyStore
	storageMtx  *sync.RWMutex
	storageChan chan map[string]msgLog

	linKv KVInterface
	seqKv KVInterface
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
	Type    string      `json:"type"`
	Offsets offset_list `json:"offsets"`
}

var wg sync.WaitGroup

const (
	LAST_OFFSET_KEY      = "global_offset_counter"
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
		storageChan: make(chan map[string]msgLog, 5),
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

		var resp any
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

func (p *Program) handleSend(body workload) sendResponse {
	o := p.getNextOffset()

	p.storageChan <- map[string]msgLog{
		body.Key: {offset: o, msg: body.Msg},
	}

	resp := sendResponse{
		Type:   "send_ok",
		Offset: o,
	}
	return resp
}

func (p *Program) storeMsg(key string, entry msgLog) {
	_, ok := p.storage[key]
	if !ok {
		p.storage[key] = &keyStore{
			key:       key,
			committed: 0,
			mtx:       &sync.RWMutex{},
			offsets:   []int64{},
		}
	}

	p.storage[key].store(entry.offset)
	err := p.seqKv.Write(context.Background(), formatMsgKey(key, entry.offset), entry.msg)
	if err != nil {
		log.Printf("storeMsg: error storing in KV %+v", err)
	}
}

func (p *Program) getNextOffset() int64 {
	for {
		current, _ := p.linKv.ReadInt(context.Background(), LAST_OFFSET_KEY)
		next := current + 1
		err := p.linKv.CompareAndSwap(context.Background(), LAST_OFFSET_KEY, current, next, true)
		if err != nil {
			switch e := err.(type) {
			case *maelstrom.RPCError:
				if e.Code == maelstrom.PreconditionFailed {
					log.Printf("getNextOffset: PreconditionFailed on CAS failed for %d -> %d", current, next)
					continue
				}
			}
		}
		return int64(next)
	}
}

func (p *Program) handlePoll(body workload) pollResponse {
	logs := message_list{}
	for k, o := range body.Offsets {
		key, ok := p.storage[k]
		logs[k] = [][2]int64{}
		if ok {
			count := 0
			ol := key.getOffsets(o)
			// log.Printf("handlePoll key offsets: %+v", ol)
			for _, offset := range ol {
				if count == MAX_POLL_LIST_LENGTH {
					break
				}
				val, err := p.seqKv.ReadInt(context.Background(), formatMsgKey(k, offset))
				// log.Printf("handlePoll: ReaInt for %s:%d -> %+v", k, offset, val)
				if err != nil {
					switch e := err.(type) {
					case *maelstrom.RPCError:
						if e.Code == maelstrom.KeyDoesNotExist {
							log.Printf("handlePoll: KeyDoesNotExist on ReaInt for %s:%d", k, offset)
							continue
						}
					}
				}
				logs[k] = append(logs[k], [2]int64{offset, int64(val)})
				count++
			}
		}
	}

	res := pollResponse{
		Type: "poll_ok",
		Msgs: logs,
	}
	return res
}

func (p *Program) handleCommitOffsets(body workload) baseResponse {
	for k, o := range body.Offsets {
		ks := p.getKeyStore(k)
		curr := ks.getCommittedOffset()
		ks.commitOffset(o)

		err := p.linKv.CompareAndSwap(context.Background(), formatCommittedOffsetKey(k), curr, o, true)
		if err != nil {
			switch e := err.(type) {
			case *maelstrom.RPCError:
				if e.Code == maelstrom.PreconditionFailed {
					log.Printf("handleCommitOffsets: PreconditionFailed error for key %+v: %s", k, err)
				}
			default:
				log.Printf("handleCommitOffsets: error for key %+v: %s", k, err)
			}
		}
	}
	return baseResponse{Type: "commit_offsets_ok"}
}

func (p *Program) handleListCommittedOffsets(body workload) listCommittedOffsetsResponse {
	res := listCommittedOffsetsResponse{
		Type:    "list_committed_offsets_ok",
		Offsets: offset_list{},
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