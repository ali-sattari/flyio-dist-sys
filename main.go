package main

import (
	"log"
	"main/broadcast"
	"main/echo"
	"main/unique_ids"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var wg sync.WaitGroup

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
	n.Handle("broadcast", brdc.GetHandle("broadcast"))
	n.Handle("broadcast_ok", brdc.GetHandle("broadcast_ok"))
	n.Handle("gossip", brdc.GetHandle("gossip"))
	n.Handle("gossip_ok", brdc.GetHandle("gossip_ok"))
	n.Handle("read", brdc.GetHandle("read"))
	n.Handle("topology", brdc.GetHandle("topology"))

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

	wg.Wait()
}
