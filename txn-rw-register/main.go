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
	"time"

	"main/pkg/txn"

	"github.com/joho/godotenv"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
)

//go:embed .env
var env string

var wg sync.WaitGroup

func main() {
	load_env()

	// observability
	ctx := context.Background()
	tracerCleanup := initTracer()
	defer tracerCleanup(ctx)

	loggerCleanup := initLogger()
	defer loggerCleanup(ctx)

	metricCleanup := initMetric()
	defer metricCleanup(ctx)

	runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second))

	logger := otelslog.NewLogger(serviceName)

	// new app
	n := txn.NewWrappedNode()
	app := txn.New(n, logger)

	rpcs := []string{"init", "txn"}
	for _, r := range rpcs {
		n.Handle(r, app.GetHandle(r))
	}

	// run & cleanup
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
