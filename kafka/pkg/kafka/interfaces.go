package kafka

import (
	"context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Message = maelstrom.Message
type Node = maelstrom.Node

// the methods we use from maelstrom.Node
type NodeInterface interface {
	ID() string
	Run() error
	Handle(rpcType string, handler maelstrom.HandlerFunc)
	Reply(msg Message, body any) error
}

// the methods we use from maelstrom.KV
type KVInterface interface {
	Read(ctx context.Context, key string) (any, error)
	ReadInt(ctx context.Context, key string) (int, error)
	Write(ctx context.Context, key string, value any) error
	CompareAndSwap(ctx context.Context, key string, oldValue, newValue any, createIfMissing bool) error
}

// Node wrapper
type MaelstromNodeWrapper struct {
	node *maelstrom.Node
}

func NewWrappedNode() *MaelstromNodeWrapper {
	return &MaelstromNodeWrapper{node: maelstrom.NewNode()}
}

func (m *MaelstromNodeWrapper) ID() string {
	return m.node.ID()
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

func (m *MaelstromKVWrapper) Read(ctx context.Context, key string) (any, error) {
	return m.kv.Read(ctx, key)
}

func (m *MaelstromKVWrapper) ReadInt(ctx context.Context, key string) (int, error) {
	return m.kv.ReadInt(ctx, key)
}

func (m *MaelstromKVWrapper) Write(ctx context.Context, key string, value any) error {
	return m.kv.Write(ctx, key, value)
}

func (m *MaelstromKVWrapper) CompareAndSwap(ctx context.Context, key string, oldValue, newValue any, createIfMissing bool) error {
	return m.kv.CompareAndSwap(ctx, key, oldValue, newValue, createIfMissing)
}
