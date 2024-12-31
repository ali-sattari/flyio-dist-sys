package broadcast

import (
	"sync"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
)

func newProg() Program {
	n := maelstrom.NewNode()
	n.Init("n1", []string{})
	p := Program{
		node:     n,
		mtx:      &sync.RWMutex{},
		bufMtx:   &sync.Mutex{},
		buffer:   make(map[string][]int64),
		gossiped: make(map[string][]gossipLog),
		topo:     map[string][]string{"n1": {"n2", "n3"}},
		ackMtx:   &sync.Mutex{},
	}
	return p
}

func TestGossipMine(t *testing.T) {
	p := newProg()
	msgs := []int64{1, 2, 3, 4, 5, 6, 7}

	p.gossip("n2", msgs)

	assert.Empty(t, p.buffer["n2"], "buffer should be empty for source node")
	assert.Equal(t, msgs, p.buffer["n3"], "buffer should be updated for node n3")
}

type MockNode struct {
	mtx  sync.Mutex
	msgs []map[string]any
}

type MaelstromNode interface {
	Send(dest string, msg any) error
}

func (mn *MockNode) Send(dest string, msg any) error {
	mn.mtx.Lock()
	defer mn.mtx.Unlock()
	mn.msgs = append(mn.msgs, msg.(map[string]any))
	return nil
}

func TestGossip(t *testing.T) {
	p := newProg()
	msgs := []int64{1, 2, 3}

	p.gossip("n1", msgs)

	expectedBuffer := map[string][]int64{
		"n2": {1, 2, 3},
		"n3": {1, 2, 3},
	}

	for k, v := range expectedBuffer {
		if !equalSlices(p.buffer[k], v) {
			t.Errorf("Expected buffer for %s to be %v, got %v", k, v, p.buffer[k])
		}
	}
}

func TestAddGossipLog(t *testing.T) {
	p := newProg()
	msgs := []int64{1, 2, 3}

	p.addGossipLog("node2", "gid-123", msgs)

	if len(p.gossiped["node2"]) != 1 {
		t.Errorf("Expected one entry in gossiped map for node2, got %d", len(p.gossiped["node2"]))
	}
}

func TestAckGossip(t *testing.T) {
	p := newProg()
	msgs := []int64{1, 2, 3}
	body := workload{
		GossipId: "gid-123",
	}

	p.addGossipLog("node2", "gid-123", msgs)
	p.ackGossip("node2", body)

	if len(p.gossiped["node2"]) != 0 {
		t.Errorf("Expected gossiped map for node2 to be empty after ack, got %d entries", len(p.gossiped["node2"]))
	}
}

func TestReceiveGossip(t *testing.T) {
	p := newProg()
	msgs := []int64{1, 2, 3}
	body := workload{
		MessageList: msgs,
		GossipId:    "gid-123",
	}

	resp := p.receiveGossip(body, maelstrom.Message{})

	if resp["type"] != "gossip_ok" {
		t.Errorf("Expected response type to be 'gossip_ok', got %v", resp["type"])
	}
	if resp["gossip_id"] != "gid-123" {
		t.Errorf("Expected gossip_id in response to be 'gid-123', got %v", resp["gossip_id"])
	}
}

// func TestFlushGossipBuffer(t *testing.T) {
// 	p := newProg()
// 	msgs := []int64{1, 2, 3}

// 	p.buffer["node2"] = msgs
// 	p.flushGossipBuffer()

// 	if len(p.node.msgs) != 1 {
// 		t.Errorf("Expected one message to be sent, got %d", len(p.node.msgs))
// 	}
// 	expectedMsg := map[string]any{
// 		"type":         "gossip",
// 		"gossip_id":    p.node.msgs[0]["gossip_id"],
// 		"message_list": msgs,
// 	}
// 	if !equalMaps(expectedMsg, p.node.msgs[0]) {
// 		t.Errorf("Expected message %v, got %v", expectedMsg, p.node.msgs[0])
// 	}
// }

// func TestResendUnackedGossips(t *testing.T) {
// 	p := newProg()
// 	p.gossiped = map[string][]gossipLog{
// 		"node2": {
// 			{at: time.Now().UnixMicro() - int64(time.Second) - 1, gossipId: "gid-123", msgs: []int64{1, 2, 3}},
// 		},
// 	}
// 	p.topo = map[string][]string{
// 		"node1": {"node2"},
// 	}
// 	msgs := []int64{1, 2, 3}

// 	p.resendUnackedGossips()

// 	if len(p.node.msgs) != 1 {
// 		t.Errorf("Expected one message to be sent, got %d", len(p.node.msgs))
// 	}
// 	expectedMsg := map[string]any{
// 		"type":         "gossip",
// 		"gossip_id":    p.gossiped["node2"][0].gossipId,
// 		"message_list": msgs,
// 	}
// 	if !equalMaps(expectedMsg, p.node.msgs[0]) {
// 		t.Errorf("Expected message %v, got %v", expectedMsg, p.node.msgs[0])
// 	}
// }

func equalSlices(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalMaps(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
}
