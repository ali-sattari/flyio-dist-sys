package main

import (
	"log"
	"main/broadcast"
	"main/echo"
	"main/unique_ids"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// echo
	ec := echo.New(n)
	n.Handle("echo", ec.Handle)

	// unique id
	unq_id, err := unique_ids.New(n)
	if err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
	n.Handle("generate", unq_id.Handle)

	// broadcast
	brdc := broadcast.New(n)
	n.Handle("broadcast", brdc.HandleBroadcast)
	n.Handle("read", brdc.HandleRead)
	n.Handle("topology", brdc.HandleTopology)

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}