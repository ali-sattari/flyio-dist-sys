package unique_ids

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"main/unique_ids/snowflaker"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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
