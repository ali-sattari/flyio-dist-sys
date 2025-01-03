package kafka

import (
	"context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Mock Node
type MockNode struct {
	NodeID       string
	RunCalled    bool
	HandleCalled bool
	ReplyCalled  bool
	LastMsg      Message
	LastBody     any
	ReturnError  error
}

func NewMockNode(id string) *MockNode {
	return &MockNode{NodeID: id}
}

func (m *MockNode) Run() error {
	m.RunCalled = true
	return m.ReturnError
}

func (m *MockNode) ID() string {
	return m.NodeID
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
	ReadCalled       int
	ReadIntCalled    int
	WriteCalled      int
	CasCalled        int
	LastKey          string
	LastValue        any
	LastOldValue     any
	LastNewValue     any
	ReturnCasError   error
	ReturnReadError  error
	ReturnWriteError error
	ReturnValueInt   int
	ReturnValue      any
	ReturnValues     map[string]any
}

func NewMockKV() *MockKV {
	return &MockKV{
		ReturnValues: map[string]any{},
	}
}

func (m *MockKV) CompareAndSwap(ctx context.Context, key string, oldValue, newValue any, createIfMissing bool) error {
	m.LastKey = key
	m.LastOldValue = oldValue
	m.LastNewValue = newValue
	m.CasCalled++

	if m.ReturnCasError != nil {
		switch e := m.ReturnCasError.(type) {
		case *maelstrom.RPCError:
			if e.Code == maelstrom.PreconditionFailed {
				m.ReturnCasError = nil
				return maelstrom.NewRPCError(maelstrom.PreconditionFailed, "CAS failed")
			}
		}
	}

	m.LastValue = newValue
	return nil
}

func (m *MockKV) Read(ctx context.Context, key string) (any, error) {
	m.ReadCalled++
	m.LastKey = key
	return m.ReturnValue, m.ReturnReadError
}

func (m *MockKV) ReadInt(ctx context.Context, key string) (int, error) {
	m.ReadIntCalled++
	m.LastKey = key

	if m.ReturnReadError != nil {
		return 0, m.ReturnReadError
	}

	if val, ok := m.ReturnValues[key]; ok {
		if intVal, ok := val.(int); ok {
			return intVal, nil
		}
	}

	return m.ReturnValueInt, m.ReturnReadError
}

func (m *MockKV) Write(ctx context.Context, key string, value any) error {
	m.WriteCalled++
	m.LastKey = key
	m.LastValue = value
	return m.ReturnWriteError
}
