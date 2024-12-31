package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"main/pkg/snowflaker"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var wg sync.WaitGroup

func main() {
	n := maelstrom.NewNode()

	unq_id, err := New(n)
	if err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
	n.Handle("generate", unq_id.Handle)

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

	wg.Wait()
}

type Program struct {
	node *maelstrom.Node
	gen  snowflaker.Generator
}

func New(n *maelstrom.Node) (Program, error) {
	nId := getNodeID(os.Getpid())
	sf, err := snowflaker.New(nId)
	if err != nil {
		return Program{}, fmt.Errorf("error making snowflaker %+v", err)
	}

	return Program{
		node: n,
		gen:  sf,
	}, nil
}

func (p *Program) Handle(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "generate_ok"
	body["id"] = p.gen.GenerateID()

	return p.node.Reply(msg, body)
}

func getNodeID(n int) int64 {
	s := fmt.Sprintf("%b", n)
	hash := crc32.ChecksumIEEE([]byte(s))
	return int64(hash & ((1 << 10) - 1)) // Constrain to 10 bits
}
