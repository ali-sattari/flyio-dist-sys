package main

import (
	"context"
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const COUNTER_KEY = "gw_counter"

type Program struct {
	node    *maelstrom.Node
	kv      *maelstrom.KV
	counter int64
}

type workload struct {
	maelstrom.MessageBody
	Type  string
	Delta int64
}

func New(node *maelstrom.Node, kv *maelstrom.KV) *Program {
	p := Program{
		node: node,
		kv:   kv,
	}
	return &p
}

func (p *Program) GetHandle(rpc_type string) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body workload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var resp map[string]any
		switch rpc_type {
		case "add":
			new_val := p.counter + body.Delta
			err := p.kv.CompareAndSwap(context.Background(), COUNTER_KEY, p.counter, new_val, true)
			if err != nil {
				return err
			}

			p.counter = new_val
			resp = map[string]any{
				"type": "add_ok",
			}
		case "read":
			val, err := p.kv.ReadInt(context.Background(), COUNTER_KEY)
			if err != nil {
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
