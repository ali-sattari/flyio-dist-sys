package kafka

import (
	"context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Message = maelstrom.Message
type Node = maelstrom.Node

// the methods we use from maelstrom.Node
type NodeInterface interface {
	Run() error
	Handle(rpcType string, handler maelstrom.HandlerFunc)
	Reply(msg Message, body any) error
}

// the methods we use from maelstrom.KV
type KVInterface interface {
	CompareAndSwap(ctx context.Context, key string, oldValue, newValue any, createIfMissing bool) error
	ReadInt(ctx context.Context, key string) (int, error)
}

// Node wrapper
type MaelstromNodeWrapper struct {
	node *maelstrom.Node
}

func NewWrappedNode() *MaelstromNodeWrapper {
	return &MaelstromNodeWrapper{node: maelstrom.NewNode()}
}

func (m *MaelstromNodeWrapper) Run() error {
	return m.node.Run()
}

func (m *MaelstromNodeWrapper) Handle(rpcType string, handler maelstrom.HandlerFunc) {
	m.node.Handle(rpcType, handler)
}

func (m *MaelstromNodeWrapper) Reply(msg maelstrom.Message, body any) error {
	return m.node.Reply(msg, body)
}

// KV wrapper
type MaelstromKVWrapper struct {
	kv *maelstrom.KV
}

func NewWrappedKV(node *MaelstromNodeWrapper, kvType string) *MaelstromKVWrapper {
	var kv *maelstrom.KV
	switch kvType {
	case "linear":
		kv = maelstrom.NewLinKV(node.node)
	case "sequential":
		kv = maelstrom.NewSeqKV(node.node)
	default:
		panic("invalid KV type")
	}
	return &MaelstromKVWrapper{kv: kv}
}

func (m *MaelstromKVWrapper) CompareAndSwap(ctx context.Context, key string, oldValue, newValue any, createIfMissing bool) error {
	return m.kv.CompareAndSwap(ctx, key, oldValue, newValue, createIfMissing)
}

func (m *MaelstromKVWrapper) ReadInt(ctx context.Context, key string) (int, error) {
	return m.kv.ReadInt(ctx, key)
}
