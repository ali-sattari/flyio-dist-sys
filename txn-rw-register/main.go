package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"main/pkg/txn"

	"github.com/joho/godotenv"
	"go.opentelemetry.io/contrib/bridges/otelslog"
)

//go:embed .env
var env string

var wg sync.WaitGroup

func main() {
	load_env()

	ctx := context.Background()
	tracerCleanup := initTracer()
	defer tracerCleanup(ctx)

	loggerCleanup := initLogger()
	defer loggerCleanup(ctx)

	logger := otelslog.NewLogger(serviceName)

	n := txn.NewWrappedNode()
	app := txn.New(n, txn.NewWrappedKV(n, "linear"), logger)

	rpcs := []string{"init", "txn", "txn_ok"}
	for _, r := range rpcs {
		n.Handle(r, app.GetHandle(r))
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

	app.Shutdown()
	wg.Wait()
}

func load_env() {
	envMap, err := godotenv.Unmarshal(env)
	if err != nil {
		fmt.Printf("error loading env: %+v", err)
		return
	}

	for key, value := range envMap {
		_ = os.Setenv(key, value)
	}
}
