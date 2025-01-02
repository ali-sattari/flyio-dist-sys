package kafka

import (
	"context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Mock Node
type MockNode struct {
	RunCalled    bool
	HandleCalled bool
	ReplyCalled  bool
	LastMsg      Message
	LastBody     any
	ReturnError  error
}

func NewMockNode() *MockNode {
	return &MockNode{}
}

func (m *MockNode) Run() error {
	m.RunCalled = true
	return m.ReturnError
}

func (m *MockNode) Handle(rpcType string, handler maelstrom.HandlerFunc) {
	m.HandleCalled = true
}

func (m *MockNode) Reply(msg Message, body any) error {
	m.ReplyCalled = true
	m.LastMsg = msg
	m.LastBody = body
	return m.ReturnError
}

// Mock KV
type MockKV struct {
	CompareAndSwapCalled bool
	ReadIntCalled        bool
	LastKey              string
	LastOldValue         any
	LastNewValue         any
	ReturnError          error
	ReturnValue          int
}

func NewMockKV() *MockKV {
	return &MockKV{}
}

func (m *MockKV) CompareAndSwap(ctx context.Context, key string, oldValue, newValue any, createIfMissing bool) error {
	m.CompareAndSwapCalled = true
	m.LastKey = key
	m.LastOldValue = oldValue
	m.LastNewValue = newValue
	return m.ReturnError
}

func (m *MockKV) ReadInt(ctx context.Context, key string) (int, error) {
	m.ReadIntCalled = true
	m.LastKey = key
	return m.ReturnValue, m.ReturnError
}
