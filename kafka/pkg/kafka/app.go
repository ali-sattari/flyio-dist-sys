package kafka

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node       *maelstrom.Node
	storage    map[string]keyStore
	lastOffset int64
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

type response struct {
	Type    string
	Msgs    map[string][]msgLog
	Offset  int64
	Offsets map[string]int64
}

func New(n *maelstrom.Node) Program {
	p := Program{
		node:       n,
		storage:    map[string]keyStore{},
		lastOffset: 100,
	}

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
	o := p.storeMsg(body.Key, body.Msg)
	resp := response{
		Type:   "send_ok",
		Offset: o,
	}
	return resp
}

func (p *Program) handlePoll(body workload) response {
	logs := map[string][]msgLog{}
	for k, o := range body.Offsets {
		key, ok := p.storage[k]
		if ok {
			logs[k] = key.read(o)
		}
	}

	res := response{
		Type: "poll_ok",
		Msgs: logs,
	}
	return res
}

func (p *Program) handleCommitOffsets(body workload) response {
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
	res := response{
		Type:    "list_committed_offsets_ok",
		Offsets: map[string]int64{},
	}
	for _, k := range body.Keys {
		if key, ok := p.storage[k]; ok {
			res.Offsets[k] = key.getCommittedOffset()
		}
	}
	return res
}

func (p *Program) getNewOffset() int64 {
	p.lastOffset += 1
	return p.lastOffset
}

func (p *Program) storeMsg(key string, msg int64) int64 {
	k, ok := p.storage[key]
	if !ok {
		p.storage[key] = keyStore{
			key:       key,
			committed: 0,
			logs:      []msgLog{},
		}
	}
	l := k.write(msg, p.getNewOffset())
	return l.offset
}
