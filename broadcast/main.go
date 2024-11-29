package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	mtx := sync.RWMutex{}
	messages := make([]int, 0)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		m, _ := body["message"].(int)
		mtx.Lock()
		messages = append(messages, m)
		mtx.Unlock()

		resp := map[string]any{
			"type": "broadcast_ok",
		}
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mtx.RLock()
		defer mtx.RUnlock()
		resp := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}
		return n.Reply(msg, resp)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		resp := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, resp)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

}

func getMessages(m []int) string {
	s := ""
	for x := range m {
		s += fmt.Sprintf("%d, ", x)
	}
	s, _ = strings.CutSuffix(s, ", ")
	s = "[" + s + "]"
	return s
}
