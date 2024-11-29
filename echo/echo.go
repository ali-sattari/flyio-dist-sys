package echo

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node *maelstrom.Node
}

func New(n *maelstrom.Node) Program {
	return Program{
		node: n,
	}
}

func (p *Program) Handle(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "echo_ok"

	return p.node.Reply(msg, body)
}
