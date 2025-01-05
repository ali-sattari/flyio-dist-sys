package kafka

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
)

func TestProcessMsg(t *testing.T) {
	tests := []struct {
		name             string
		key              string
		offset           int64
		initialOffsets   []int64
		expectedOffsets  []int64
		expectedNewEntry bool // Whether a new offset should be added
	}{
		{
			name:             "New offset",
			key:              "k1",
			offset:           1,
			initialOffsets:   []int64{},
			expectedOffsets:  []int64{1},
			expectedNewEntry: true,
		},
		{
			name:             "Existing offset",
			key:              "k1",
			offset:           1,
			initialOffsets:   []int64{1, 2, 3},
			expectedOffsets:  []int64{1, 2, 3},
			expectedNewEntry: false,
		},
		{
			name:             "New offset in existing keyStore",
			key:              "k1",
			offset:           4,
			initialOffsets:   []int64{1, 2, 3},
			expectedOffsets:  []int64{1, 2, 3, 4},
			expectedNewEntry: true,
		},
		{
			name:             "Empty initial offsets",
			key:              "k1",
			offset:           1,
			initialOffsets:   []int64{},
			expectedOffsets:  []int64{1},
			expectedNewEntry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Program{
				storage:    make(map[string]*keyStore),
				storageMtx: &sync.RWMutex{},
			}

			ks := &keyStore{key: tt.key, committed: 0, mtx: &sync.RWMutex{}, offsets: tt.initialOffsets}
			p.storage[tt.key] = ks

			// Call the processMsg function
			newEntry := p.processMsg(tt.key, tt.offset)

			// Assertions
			ks.mtx.RLock()
			defer ks.mtx.RUnlock()
			assert.Equal(t, tt.expectedOffsets, ks.offsets)
			assert.Equal(t, tt.expectedNewEntry, newEntry)

		})
	}
}

func TestGossip(t *testing.T) {
	tests := []struct {
		name           string
		node           string
		src            string
		key            string
		topology       []string
		offset         int64
		initialBuffer  map[string]offset_list
		expectedBuffer map[string]offset_list
	}{
		{
			name:          "Gossip to single node",
			node:          "n2",
			src:           "n1",
			topology:      []string{"n3"},
			key:           "k1",
			offset:        42,
			initialBuffer: map[string]offset_list{},
			expectedBuffer: map[string]offset_list{
				"n3": {
					"k1": {42},
				},
			},
		},
		{
			name:          "Gossip to multiple nodes",
			node:          "n2",
			src:           "n1",
			key:           "k1",
			topology:      []string{"n3", "n4"},
			offset:        42,
			initialBuffer: map[string]offset_list{},
			expectedBuffer: map[string]offset_list{
				"n3": {"k1": {42}},
				"n4": {"k1": {42}},
			},
		},
		{
			name:          "No gossip to source node",
			node:          "n2",
			src:           "n1",
			key:           "k1",
			topology:      []string{"n1", "n3"},
			offset:        42,
			initialBuffer: map[string]offset_list{},
			expectedBuffer: map[string]offset_list{
				"n3": {"k1": {42}},
			},
		},
		{
			name:     "Append to existing offsets",
			node:     "n2",
			src:      "n1",
			topology: []string{"n3"},
			key:      "k1",
			offset:   42,
			initialBuffer: map[string]offset_list{
				"n3": {"k1": {1, 2, 3}},
			},
			expectedBuffer: map[string]offset_list{
				"n3": {"k1": {1, 2, 3, 42}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Program{
				node:   NewMockNode(tt.node),
				topo:   tt.topology,
				buffer: map[string]offset_list{},
				bufMtx: &sync.Mutex{},
			}

			for k := range tt.initialBuffer {
				p.buffer[k] = tt.initialBuffer[k]
			}

			p.gossip(tt.src, tt.key, tt.offset)

			for node, expectedOffset := range tt.expectedBuffer {
				actualOffset := p.buffer[node]
				assert.Equal(t, expectedOffset, actualOffset, "Incorrect offset gossiped to node %s", node)
			}
		})
	}
}

func TestReceiveGossip(t *testing.T) {
	tests := []struct {
		name              string
		node              string
		key               string
		gossipOffsets     []int64
		initialOffsets    []int64
		mockNode          *MockNode
		expectedKeystores map[string]*keyStore
		expectedRes       receiveGossipResponse
	}{
		{
			name:           "New offset",
			node:           "n1",
			key:            "key1",
			gossipOffsets:  []int64{100},
			initialOffsets: []int64{},
			mockNode:       NewMockNode("n1"),
			expectedKeystores: map[string]*keyStore{
				"key1": {offsets: []int64{100}},
			},
			expectedRes: receiveGossipResponse{
				Type:     "gossip_ok",
				GossipId: "gossip_id_1",
			},
		},
		{
			name:           "Existing offset",
			node:           "n1",
			key:            "key1",
			gossipOffsets:  []int64{100},
			initialOffsets: []int64{100},
			mockNode:       NewMockNode("n1"),
			expectedKeystores: map[string]*keyStore{
				"key1": {offsets: []int64{100}},
			},
			expectedRes: receiveGossipResponse{
				Type:     "gossip_ok",
				GossipId: "gossip_id_1",
			},
		},
		{
			name:           "Multiple offsets, some new",
			node:           "n1",
			key:            "key1",
			gossipOffsets:  []int64{100, 102, 105},
			initialOffsets: []int64{100, 101},
			mockNode:       NewMockNode("n1"),
			expectedKeystores: map[string]*keyStore{
				"key1": {offsets: []int64{100, 101, 102, 105}},
			},
			expectedRes: receiveGossipResponse{
				Type:     "gossip_ok",
				GossipId: "gossip_id_1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Program{
				node:       tt.mockNode,
				topo:       []string{"n2", "n3"},
				bufMtx:     &sync.Mutex{},
				buffer:     map[string]offset_list{},
				storageMtx: &sync.RWMutex{},
				storage: map[string]*keyStore{
					"key1": {offsets: tt.initialOffsets, mtx: &sync.RWMutex{}},
				},
			}

			body := workload{
				Type:          "gossip",
				GossipId:      "gossip_id_1",
				Key:           tt.key,
				GossipOffsets: tt.gossipOffsets,
			}

			msg := maelstrom.Message{
				Src: "n2",
			}

			res := p.receiveGossip(body, msg)

			assert.Equal(t, tt.expectedRes, res)

			// Assert the keystores
			for k, v := range tt.expectedKeystores {
				assert.ElementsMatch(t, v.offsets, p.storage[k].offsets)
			}
		})
	}
}

func TestAddGossipLog(t *testing.T) {
	tests := []struct {
		name         string
		node         string
		key          string
		id           string
		offsets      []int64
		initialLogs  map[string][]gossipLog
		expectedLogs map[string][]gossipLog
	}{
		{
			name:        "Add first log",
			node:        "n1",
			key:         "k1",
			id:          "id111",
			offsets:     []int64{1, 2, 3},
			initialLogs: map[string][]gossipLog{},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", offsets: []int64{1, 2, 3}},
				},
			},
		},
		{
			name:    "Append new log",
			node:    "n1",
			key:     "k1",
			id:      "id222",
			offsets: []int64{4, 5, 6},
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", offsets: []int64{1, 2, 3}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", offsets: []int64{1, 2, 3}},
					{gossipId: "id222", key: "k1", offsets: []int64{4, 5, 6}},
				},
			},
		},
		{
			name:    "Append duplicate offset",
			node:    "n1",
			key:     "k1",
			id:      "id222",
			offsets: []int64{1, 2, 3},
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", offsets: []int64{1, 2, 3}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", offsets: []int64{1, 2, 3}},
					{gossipId: "id222", key: "k1", offsets: []int64{1, 2, 3}},
				},
			},
		},
		{
			name:    "Append with different key",
			node:    "n1",
			key:     "k2",
			id:      "id222",
			offsets: []int64{11, 12, 13},
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", offsets: []int64{1, 2, 3}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", offsets: []int64{1, 2, 3}},
					{gossipId: "id222", key: "k2", offsets: []int64{11, 12, 13}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Program{
				gossiped: tt.initialLogs,
				ackMtx:   &sync.Mutex{},
			}

			p.addGossipLog(tt.node, tt.id, tt.key, tt.offsets)

			assert.Empty(
				t,
				cmp.Diff(
					tt.expectedLogs,
					p.gossiped,
					cmp.AllowUnexported(gossipLog{}),
					cmpopts.IgnoreFields(gossipLog{}, "at"),
				),
				"Diff between expected and p.gossiped actual",
			)
		})
	}
}

func TestAckGossip(t *testing.T) {
	tests := []struct {
		name         string
		src          string
		id           string
		initialLogs  map[string][]gossipLog
		expectedLogs map[string][]gossipLog
	}{
		{
			name: "Ack existing log",
			src:  "n1",
			id:   "gossip111",
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", offsets: []int64{1, 2, 3}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {},
			},
		},
		{
			name: "Ack non-existent log",
			src:  "n1",
			id:   "gossip222",
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", offsets: []int64{1, 2, 3}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", offsets: []int64{1, 2, 3}},
				},
			},
		},
		{
			name: "Ack one from many logs same node",
			src:  "n1",
			id:   "gossip222",
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", offsets: []int64{1, 2, 3}},
					{gossipId: "gossip222", key: "k2", offsets: []int64{42, 101, 60}},
					{gossipId: "gossip333", key: "k5", offsets: []int64{4, 5, 6}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", offsets: []int64{1, 2, 3}},
					{gossipId: "gossip333", key: "k5", offsets: []int64{4, 5, 6}},
				},
			},
		},
		{
			name: "Ack one from many logs diff nodes",
			src:  "n2",
			id:   "gossip222",
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", offsets: []int64{1, 2, 3}},
				},
				"n2": {
					{gossipId: "gossip222", key: "k2", offsets: []int64{42, 101, 60}},
					{gossipId: "gossip333", key: "k5", offsets: []int64{4, 5, 6}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", offsets: []int64{1, 2, 3}},
				},
				"n2": {
					{gossipId: "gossip333", key: "k5", offsets: []int64{4, 5, 6}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Program{
				gossiped: tt.initialLogs,
				ackMtx:   &sync.Mutex{},
			}

			body := workload{
				GossipId: tt.id,
			}
			msg := maelstrom.Message{
				Src: tt.src,
			}

			p.ackGossip(body, msg)

			assert.Empty(
				t,
				cmp.Diff(
					tt.expectedLogs,
					p.gossiped,
					cmp.AllowUnexported(gossipLog{}),
					cmpopts.IgnoreFields(gossipLog{}, "at"),
				),
				"Diff between expected and p.gossiped actual",
			)

		})
	}
}

func TestFlushGossipBuffer(t *testing.T) {
	tests := []struct {
		name             string
		node             string
		lastFlush        time.Time
		initialBuffer    map[string]offset_list
		gossiped         map[string][]gossipLog
		expectedGossiped map[string][]gossipLog
		expectedBuffer   map[string]offset_list
		mockNode         *MockNode
	}{
		{
			name:      "Empty buffer",
			node:      "n1",
			lastFlush: time.Now().Add(-1 * bufferAge),
			initialBuffer: map[string]offset_list{
				"n2": {},
			},
			gossiped:         map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{},
			expectedBuffer: map[string]offset_list{
				"n2": {},
			},
			mockNode: NewMockNode("n1"),
		},
		{
			name:      "Single node, single key",
			node:      "n1",
			lastFlush: time.Now().Add(-1 * bufferAge),
			initialBuffer: map[string]offset_list{
				"n2": {
					"k1": {42},
				},
			},
			gossiped: map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{
				"n2": {
					{key: "k1", offsets: []int64{42}},
				},
			},
			expectedBuffer: map[string]offset_list{
				"n2": {},
			},
			mockNode: NewMockNode("n1"),
		},
		{
			name:      "Single node, multiple keys",
			node:      "n1",
			lastFlush: time.Now().Add(-10 * bufferAge),
			initialBuffer: map[string]offset_list{
				"n2": {
					"k1": {42},
					"k2": {101},
				},
			},
			gossiped: map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{
				"n2": {
					{key: "k1", offsets: []int64{42}},
					{key: "k2", offsets: []int64{101}},
				},
			},
			expectedBuffer: map[string]offset_list{
				"n2": {},
			},
			mockNode: NewMockNode("n1"),
		},
		{
			name:      "Multiple nodes, multiple keys",
			node:      "n1",
			lastFlush: time.Now().Add(-10 * bufferAge), // needs a bit more time
			initialBuffer: map[string]offset_list{
				"n2": {
					"k1": {42, 43},
					"k2": {101},
				},
				"n3": {
					"k1": {42, 44},
					"k3": {202},
				},
			},
			gossiped: map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{
				"n2": {
					{key: "k1", offsets: []int64{42, 43}},
					{key: "k2", offsets: []int64{101}},
				},
				"n3": {
					{key: "k1", offsets: []int64{42, 44}},
					{key: "k3", offsets: []int64{202}},
				},
			},
			expectedBuffer: map[string]offset_list{
				"n2": {},
				"n3": {},
			},
			mockNode: NewMockNode("n1"),
		},
		{
			name:      "No flush",
			node:      "n1",
			lastFlush: time.Now(),
			initialBuffer: map[string]offset_list{
				"n2": {
					"k1": {42},
				},
			},
			gossiped:         map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{},
			expectedBuffer: map[string]offset_list{
				"n2": {
					"k1": {42},
				},
			},
			mockNode: NewMockNode("n1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Program{
				node:      tt.mockNode,
				topo:      []string{"n2", "n3"},
				buffer:    tt.initialBuffer,
				bufMtx:    &sync.Mutex{},
				gossiped:  tt.gossiped,
				ackMtx:    &sync.Mutex{},
				lastFlush: tt.lastFlush,
				flushMtx:  &sync.Mutex{},
			}

			p.flushGossipBuffer()

			// Assert that the buffer has been updated as expected
			if !cmp.Equal(p.buffer, tt.expectedBuffer) {
				t.Errorf("flushGossipBuffer() buffer mismatch (-want +got):\n%s", cmp.Diff(tt.expectedBuffer, p.buffer))
			}

			// Assert that the gossiped logs have been updated as expected
			for node, logs := range p.gossiped {
				sortGossipLogs(logs)
				p.gossiped[node] = logs
			}
			for node, logs := range tt.expectedGossiped {
				sortGossipLogs(logs)
				tt.expectedGossiped[node] = logs
			}
			for n, eg := range tt.expectedGossiped {
				assert.Empty(
					t,
					cmp.Diff(
						eg,
						p.gossiped[n],
						cmp.AllowUnexported(gossipLog{}),
						cmpopts.IgnoreFields(gossipLog{}, "at", "gossipId"),
					),
					"Diff between expected and p.gossiped actual",
				)
			}

			// Assert that the correct number of messages were sent
			expectedSendCalled := 0
			for _, a := range tt.expectedGossiped {
				expectedSendCalled += len(a)
			}
			assert.Equal(t, expectedSendCalled, tt.mockNode.SendCalled)

			// Assert that the last flush time has been updated
			if len(tt.expectedGossiped) > 0 {
				assert.Truef(t, p.lastFlush.After(tt.lastFlush), "p.lastFLush %+v vs %+v", p.lastFlush, tt.lastFlush)
			}
		})
	}
}

func TestResendUnackedGossips(t *testing.T) {
	now := time.Now().UnixMicro()
	old := time.Now().Add(-1 * time.Minute).UnixMicro()

	tests := []struct {
		name              string
		node              string
		gossiped          map[string][]gossipLog
		expectedSendCalls int
		mockNode          *MockNode
	}{
		{
			name: "No unacked gossips",
			node: "n1",
			gossiped: map[string][]gossipLog{
				"n2": {
					{gossipId: "g1", at: now},
				},
			},
			expectedSendCalls: 0,
			mockNode:          NewMockNode("n1"),
		},
		{
			name: "Single unacked gossip",
			node: "n1",
			gossiped: map[string][]gossipLog{
				"n2": {
					{gossipId: "g1", at: old, key: "k1", offsets: []int64{42}},
				},
			},
			expectedSendCalls: 1,
			mockNode:          NewMockNode("n1"),
		},
		{
			name: "Multiple unacked gossips",
			node: "n1",
			gossiped: map[string][]gossipLog{
				"n2": {
					{gossipId: "g1", at: old, key: "k1", offsets: []int64{42}},
					{gossipId: "g2", at: old, key: "k2", offsets: []int64{101}},
				},
			},
			expectedSendCalls: 2,
			mockNode:          NewMockNode("n1"),
		},
		{
			name: "Unacked gossips to multiple nodes",
			node: "n1",
			gossiped: map[string][]gossipLog{
				"n2": {
					{gossipId: "g1", at: old, key: "k1", offsets: []int64{42}},
				},
				"n3": {
					{gossipId: "g2", at: old, key: "k2", offsets: []int64{101}},
				},
			},
			expectedSendCalls: 2,
			mockNode:          NewMockNode("n1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Program{
				node:     tt.mockNode,
				gossiped: tt.gossiped,
				ackMtx:   &sync.Mutex{},
			}

			p.resendUnackedGossips()

			assert.Equal(t, tt.expectedSendCalls, tt.mockNode.SendCalled, "Should have called send N times")
		})
	}
}

func sortGossipLogs(logs []gossipLog) {
	sort.Slice(logs, func(i, j int) bool {
		return logs[i].key < logs[j].key
	})
}
