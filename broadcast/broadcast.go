package broadcast

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node     *maelstrom.Node
	mtx      *sync.RWMutex
	messages []int64
	topo     map[string][]string
}

type Workload struct {
	Type     string
	Message  int64
	Topology map[string][]string
}

func New(n *maelstrom.Node) Program {
	return Program{
		node: n,
		mtx:  &sync.RWMutex{},
	}
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
		return resp
	}

	p.mtx.Lock()
	p.messages = append(p.messages, m)
	p.mtx.Unlock()

	p.gossip(m)

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

func (p *Program) gossip(msg int64) {
	targets := p.topo[p.node.ID()]

	for _, t := range targets {
		p.node.Send(t, Workload{
			Type:    "broadcast",
			Message: msg,
		})
	}
}

func (p *Program) haveMessage(msg int64) bool {
	for _, m := range p.messages {
		if m == msg {
			return true
		}
	}
	return false
}
