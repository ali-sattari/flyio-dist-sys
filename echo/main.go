package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var wg sync.WaitGroup

func main() {
	n := maelstrom.NewNode()

	// echo
	ec := New(n)
	n.Handle("echo", ec.Handle)

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

	wg.Wait()
}

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
