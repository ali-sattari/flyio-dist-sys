package broadcast

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node     *maelstrom.Node
	mtx      *sync.RWMutex
	messages []int
}

func New(n *maelstrom.Node) Program {
	return Program{
		node: n,
		mtx:  &sync.RWMutex{},
	}
}

func (p *Program) HandleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	m, _ := body["message"].(int)
	p.mtx.Lock()
	p.messages = append(p.messages, m)
	p.mtx.Unlock()

	resp := map[string]any{
		"type": "broadcast_ok",
	}
	return p.node.Reply(msg, resp)
}

func (p *Program) HandleRead(msg maelstrom.Message) error {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	resp := map[string]any{
		"type":     "read_ok",
		"messages": p.messages,
	}
	return p.node.Reply(msg, resp)
}

func (p *Program) HandleTopology(msg maelstrom.Message) error {
	resp := map[string]any{
		"type": "topology_ok",
	}
	return p.node.Reply(msg, resp)
}
