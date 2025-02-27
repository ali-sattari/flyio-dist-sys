package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const COUNTER_KEY = "gw_counter"

type Program struct {
	node    *maelstrom.Node
	kv      *maelstrom.KV
	mtx     *sync.RWMutex
	counter int64
	log     *slog.Logger
}

type workload struct {
	maelstrom.MessageBody
	Type  string
	Delta int64
}

func New(node *maelstrom.Node, kv *maelstrom.KV, logger *slog.Logger) *Program {
	p := Program{
		node:    node,
		kv:      kv,
		log:     logger,
		mtx:     &sync.RWMutex{},
		counter: 0,
	}
	return &p
}

func (p *Program) GetHandle(rpc_type string) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		ctx, span := otel.Tracer("g-counter").Start(
			context.Background(),
			fmt.Sprintf("handle_%s", rpc_type),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("node", p.node.ID())),
		)
		defer span.End()

		var body workload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			span.RecordError(err)
			return err
		}

		var resp map[string]any
		switch rpc_type {
		// init
		case "init":
			resp = map[string]any{
				"type": "init_ok",
			}
			p.log = p.log.With("node", p.node.ID())

		// add
		case "add":
			if body.Delta > 0 {
				p.mtx.Lock()
				defer p.mtx.Unlock()

				new_val := p.counter + body.Delta
				// err := p.kv.Write(ctx, getNodeKey(p.node.ID()), new_val)
				err := p.kv.CompareAndSwap(ctx, getNodeKey(p.node.ID()), p.counter, new_val, true)
				if err != nil {
					span.RecordError(err)
					p.log.Error(
						fmt.Sprintf("add %d -> %d", p.counter, new_val),
						slog.Int64("counter", p.counter),
						slog.Int64("new_value", new_val),
						slog.String("error", err.Error()),
					)

					return err
				}

				p.counter = new_val
			}

			resp = map[string]any{
				"type": "add_ok",
			}

		// read
		case "read":
			p.mtx.RLock()
			defer p.mtx.RUnlock()

			val, err := p.readNodeCounters(ctx)
			if err {
				err := fmt.Errorf("could not read counter values")
				span.RecordError(err)
				span.SetStatus(codes.Error, codes.Error.String())
				span.SetAttributes(attribute.Int64("counter", p.counter))

				p.log.Error(
					err.Error(),
					slog.Int64("counter", p.counter),
					slog.Int64("read_value", val),
				)
				return err
			}

			val = max(val, p.counter)
			resp = map[string]any{
				"type":  "read_ok",
				"value": val,
			}
		}

		return p.node.Reply(msg, resp)
	}
}

func (p *Program) readNodeCounters(ctx context.Context) (int64, bool) {
	total := 0
	e := true
	for _, n := range p.node.NodeIDs() {
		val, err := p.kv.ReadInt(ctx, getNodeKey(n))
		if err == nil {
			e = false
		}
		total += val
	}
	return int64(total), e
}

func getNodeKey(id string) string {
	return fmt.Sprintf("%s_%s", COUNTER_KEY, id)
}
