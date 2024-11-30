package broadcast

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node     *maelstrom.Node
	mtx      *sync.RWMutex
	messages []float64
}

func New(n *maelstrom.Node) Program {
	return Program{
		node: n,
		mtx:  &sync.RWMutex{},
	}
}

func (p *Program) Handle(rpc_type string) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body map[string]any
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
		}

		return p.node.Reply(msg, resp)
	}

}

func (p *Program) broadcast(body map[string]any) map[string]any {
	m := body["message"].(float64)
	p.mtx.Lock()
	p.messages = append(p.messages, m)
	p.mtx.Unlock()

	resp := map[string]any{
		"type": "broadcast_ok",
	}
	return resp
}

func (p *Program) read(body map[string]any) map[string]any {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	resp := map[string]any{
		"type":     "read_ok",
		"messages": p.messages,
	}
	return resp
}

func (p *Program) topology(body map[string]any) map[string]any {
	resp := map[string]any{
		"type": "topology_ok",
	}
	return resp
}
