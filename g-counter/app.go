package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const COUNTER_KEY = "gw_counter"

type Program struct {
	node    *maelstrom.Node
	kv      *maelstrom.KV
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
		node: node,
		kv:   kv,
		log:  logger,
	}
	return &p
}

func (p *Program) GetHandle(rpc_type string) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		ctx, span := otel.Tracer("g-counter").Start(
			context.Background(),
			fmt.Sprintf("handle_%s", rpc_type),
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()

		var body workload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			span.RecordError(err)
			return err
		}

		var resp map[string]any
		switch rpc_type {
		case "add":
			new_val := p.counter + body.Delta
			err := p.kv.CompareAndSwap(ctx, COUNTER_KEY, p.counter, new_val, true)
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
			resp = map[string]any{
				"type": "add_ok",
			}
		case "read":
			val, err := p.kv.ReadInt(ctx, COUNTER_KEY)
			if err != nil {
				span.RecordError(err)
				p.log.Error("error in read", slog.String("error", err.Error()))
				return err
			}

			resp = map[string]any{
				"type":  "read_ok",
				"value": val,
			}
		}

		return p.node.Reply(msg, resp)
	}
}
