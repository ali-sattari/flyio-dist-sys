package main

import (
	"echo/unique-ids/snowflaker"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	nId := getNodeID(os.Getpid())
	sf, err := snowflaker.New(nId)
	if err != nil {
		log.Printf("error making snowflaker %+v", err)
	}

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type.
		body["type"] = "generate_ok"
		body["id"] = sf.GenerateID()

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func getNodeID(n int) int64 {
	s := fmt.Sprintf("%b", n)
	hash := crc32.ChecksumIEEE([]byte(s))
	return int64(hash & ((1 << 10) - 1)) // Constrain to 10 bits
}
