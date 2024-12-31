package main

import (
	"log"
	"os"
	"sync"

	"main/pkg/kafka"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var wg sync.WaitGroup

func main() {
	n := maelstrom.NewNode()

	kfk := kafka.New(n)

	rpcs := []string{"send", "poll", "commit_offsets", "list_committed_offsets"}
	for _, r := range rpcs {
		n.Handle(r, kfk.GetHandle(r))
	}

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

	wg.Wait()
}
