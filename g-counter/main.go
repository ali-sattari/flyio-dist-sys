package main

import (
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var wg sync.WaitGroup

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	goc := New(n, kv)
	n.Handle("add", goc.GetHandle("add"))
	n.Handle("read", goc.GetHandle("read"))

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

	wg.Wait()
}
