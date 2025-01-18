package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/joho/godotenv"
	"go.opentelemetry.io/contrib/bridges/otelslog"
)

//go:embed .env
var env string

var wg sync.WaitGroup

func main() {
	load_env()

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	ctx := context.Background()
	tracerCleanup := initTracer()
	defer tracerCleanup(ctx)

	loggerCleanup := initLogger()
	defer loggerCleanup(ctx)

	logger := otelslog.NewLogger(serviceName)

	goc := New(n, kv, logger)
	n.Handle("add", goc.GetHandle("add"))
	n.Handle("read", goc.GetHandle("read"))

	if err := n.Run(); err != nil {
		logger.Error("main: %s", err)
		os.Exit(1)
	}

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
