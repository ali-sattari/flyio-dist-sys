package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"main/pkg/kafka"
)

var wg sync.WaitGroup

func main() {
	n := kafka.NewWrappedNode()
	kfk := kafka.New(n, kafka.NewWrappedKV(n, "linear"), kafka.NewWrappedKV(n, "sequential"))

	rpcs := []string{"init", "send", "poll", "commit_offsets", "list_committed_offsets", "gossip", "gossip_ok"}
	for _, r := range rpcs {
		n.Handle(r, kfk.GetHandle(r))
	}

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := n.Run(); err != nil {
			log.Printf("ERROR: %s", err)
			os.Exit(1)
		}
	}()

	select {
	case sig := <-signalChan:
		log.Printf("Received termination signal: %v", sig)
	}

	kfk.Shutdown()
	wg.Wait()
}
