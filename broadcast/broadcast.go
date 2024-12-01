package broadcast

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node     *maelstrom.Node
	mtx      *sync.RWMutex
	messages []int64
	topo     map[string][]string
	sent     map[string][]sentLog
	ackMtx   *sync.Mutex
}

type sentLog struct {
	at    int64
	msgId int
	msg   int64
}

type Workload struct {
	maelstrom.MessageBody
	Type     string
	Message  int64
	Topology map[string][]string
}

func New(n *maelstrom.Node) Program {
	p := Program{
		node:   n,
		mtx:    &sync.RWMutex{},
		ackMtx: &sync.Mutex{},
		sent:   make(map[string][]sentLog),
	}
	p.resendUnackedLog()
	return p
}

func (p *Program) GetHandle(rpc_type string) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body Workload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var resp map[string]any
		switch rpc_type {
		case "broadcast":
			resp = p.broadcast(body)
		case "read":
			resp = p.read(body)
		case "topology":
			resp = p.topology(body)
		case "broadcast_ok":
			p.ackSentLog(msg.Src, body)
			return nil
		}

		return p.node.Reply(msg, resp)
	}

}

func (p *Program) broadcast(body Workload) map[string]any {
	m := body.Message
	resp := map[string]any{
		"type": "broadcast_ok",
	}

	if p.haveMessage(m) {
		log.Printf("already have message %d, ignoring: %+v", m, body)
		return resp
	}

	p.mtx.Lock()
	p.messages = append(p.messages, m)
	p.mtx.Unlock()

	p.gossip(m, body)
	return resp
}

func (p *Program) read(body Workload) map[string]any {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	resp := map[string]any{
		"type":     "read_ok",
		"messages": p.messages,
	}
	return resp
}

func (p *Program) topology(body Workload) map[string]any {
	p.topo = body.Topology
	resp := map[string]any{
		"type": "topology_ok",
	}
	return resp
}

func (p *Program) gossip(msg int64, body Workload) {
	targets := p.topo[p.node.ID()]

	for _, t := range targets {
		p.node.Send(t, Workload{
			Type:    "broadcast",
			Message: msg,
		})
		p.addSentLog(t, body.MsgID, msg)
	}
}

func (p *Program) haveMessage(msg int64) bool {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	for _, m := range p.messages {
		if m == msg {
			return true
		}
	}
	return false
}

func (p *Program) addSentLog(node string, id int, msg int64) {
	p.ackMtx.Lock()
	defer p.ackMtx.Unlock()
	if _, ok := p.sent[node]; !ok {
		p.sent[node] = make([]sentLog, 0)
	}
	p.sent[node] = append(
		p.sent[node],
		sentLog{
			at:    time.Now().UnixMicro(),
			msgId: id,
			msg:   msg,
		},
	)
}

func (p *Program) ackSentLog(src string, body Workload) {
	p.ackMtx.Lock()
	defer p.ackMtx.Unlock()
	id := body.InReplyTo
	for i, l := range p.sent[src] {
		if l.msgId == id {
			p.sent[src] = remove(p.sent[src], i)
		}
	}
}

func remove(s []sentLog, i int) []sentLog {
	if i > len(s) {
		return s
	}
	if i == len(s) {
		return s[:len(s)-1]
	}
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func (p *Program) resendUnackedLog() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				for n, sl := range p.sent {
					for _, l := range sl {
						if time.Since(time.UnixMicro(l.at)) >= time.Second {
							p.node.Send(n, Workload{
								Type:    "broadcast",
								Message: l.msg,
							})
						}
					}
				}
			}
		}
	}()
}
