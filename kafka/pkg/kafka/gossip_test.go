package kafka

import (
	"maps"
	"slices"
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
		message          MsgLog
		initialMessages  map[int64]int64
		expectedMessages map[int64]int64
		expectedNewEntry bool // Whether a new offset should be added
	}{
		{
			name:             "New message",
			key:              "k1",
			message:          MsgLog{1, 10},
			initialMessages:  map[int64]int64{},
			expectedMessages: map[int64]int64{1: 10},
			expectedNewEntry: true,
		},
		{
			name:             "Existing message",
			key:              "k1",
			message:          MsgLog{1, 10},
			initialMessages:  map[int64]int64{1: 10, 2: 20, 3: 30},
			expectedMessages: map[int64]int64{1: 10, 2: 20, 3: 30},
			expectedNewEntry: false,
		},
		{
			name:             "New message in existing keyStore",
			key:              "k1",
			message:          MsgLog{4, 40},
			initialMessages:  map[int64]int64{1: 10, 2: 20, 3: 30},
			expectedMessages: map[int64]int64{1: 10, 2: 20, 3: 30, 4: 40},
			expectedNewEntry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Program{
				storage:    make(map[string]*keyStore),
				storageMtx: &sync.RWMutex{},
			}

			ks := &keyStore{
				key:       tt.key,
				committed: 0,
				mtx:       &sync.RWMutex{},
				offsets:   slices.Sorted(maps.Keys(tt.initialMessages)),
				messages:  tt.initialMessages,
			}
			p.storage[tt.key] = ks

			// Call the processMsg function
			newEntry := p.processMsg(tt.key, tt.message)

			// Assertions
			ks.mtx.RLock()
			defer ks.mtx.RUnlock()
			assert.Equal(t, tt.expectedMessages, ks.messages)
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
		message        MsgLog
		initialBuffer  map[string]msg_log_list
		expectedBuffer map[string]msg_log_list
	}{
		{
			name:          "Gossip to single node",
			node:          "n2",
			src:           "n1",
			topology:      []string{"n3"},
			key:           "k1",
			message:       MsgLog{Offset: 42, Msg: 1},
			initialBuffer: map[string]msg_log_list{},
			expectedBuffer: map[string]msg_log_list{
				"n3": {
					"k1": {{42, 1}},
				},
			},
		},
		{
			name:          "Gossip to multiple nodes",
			node:          "n2",
			src:           "n1",
			key:           "k1",
			topology:      []string{"n3", "n4"},
			message:       MsgLog{Offset: 42, Msg: 1},
			initialBuffer: map[string]msg_log_list{},
			expectedBuffer: map[string]msg_log_list{
				"n3": {
					"k1": {{42, 1}},
				},
				"n4": {
					"k1": {{42, 1}},
				},
			},
		},
		{
			name:          "No gossip to source node",
			node:          "n2",
			src:           "n1",
			key:           "k1",
			topology:      []string{"n1", "n3"},
			message:       MsgLog{Offset: 42, Msg: 1},
			initialBuffer: map[string]msg_log_list{},
			expectedBuffer: map[string]msg_log_list{
				"n3": {
					"k1": {{42, 1}},
				},
			},
		},
		{
			name:     "Append to existing messages",
			node:     "n2",
			src:      "n1",
			topology: []string{"n3"},
			key:      "k1",
			message:  MsgLog{Offset: 42, Msg: 420},
			initialBuffer: map[string]msg_log_list{
				"n3": {
					"k1": {
						{1, 10}, {2, 20}, {3, 20},
					},
				},
			},
			expectedBuffer: map[string]msg_log_list{
				"n3": {
					"k1": {
						{1, 10}, {2, 20}, {3, 20}, {42, 420},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Program{
				node:   NewMockNode(tt.node),
				topo:   tt.topology,
				buffer: map[string]msg_log_list{},
				bufMtx: &sync.Mutex{},
			}

			for k := range tt.initialBuffer {
				p.buffer[k] = tt.initialBuffer[k]
			}

			p.gossip(tt.src, tt.key, tt.message)

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
		gossipMessages    []MsgLog
		initialMessages   map[int64]int64
		mockNode          *MockNode
		expectedKeystores map[string]*keyStore
		expectedRes       receiveGossipResponse
	}{
		{
			name:            "New offset",
			node:            "n1",
			key:             "key1",
			gossipMessages:  []MsgLog{{1, 100}},
			initialMessages: map[int64]int64{},
			mockNode:        NewMockNode("n1"),
			expectedKeystores: map[string]*keyStore{
				"key1": {messages: map[int64]int64{1: 100}},
			},
			expectedRes: receiveGossipResponse{
				Type:     "gossip_ok",
				GossipId: "gossip_id_1",
			},
		},
		{
			name:            "Existing offset",
			node:            "n1",
			key:             "key1",
			gossipMessages:  []MsgLog{{1, 100}},
			initialMessages: map[int64]int64{1: 100},
			mockNode:        NewMockNode("n1"),
			expectedKeystores: map[string]*keyStore{
				"key1": {messages: map[int64]int64{1: 100}},
			},
			expectedRes: receiveGossipResponse{
				Type:     "gossip_ok",
				GossipId: "gossip_id_1",
			},
		},
		{
			name:            "Multiple offsets, some new",
			node:            "n1",
			key:             "key1",
			gossipMessages:  []MsgLog{{1, 101}, {3, 103}, {5, 105}},
			initialMessages: map[int64]int64{1: 101, 2: 102},
			mockNode:        NewMockNode("n1"),
			expectedKeystores: map[string]*keyStore{
				"key1": {messages: map[int64]int64{1: 101, 2: 102, 3: 103, 5: 105}},
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
				buffer:     map[string]msg_log_list{},
				storageMtx: &sync.RWMutex{},
				storage: map[string]*keyStore{
					"key1": {
						offsets:  slices.Sorted(maps.Keys(tt.initialMessages)),
						messages: tt.initialMessages,
						mtx:      &sync.RWMutex{},
					},
				},
			}

			body := workload{
				Type:           "gossip",
				GossipId:       "gossip_id_1",
				Key:            tt.key,
				GossipMessages: tt.gossipMessages,
			}

			msg := maelstrom.Message{
				Src: "n2",
			}

			res := p.receiveGossip(body, msg)

			assert.Equal(t, tt.expectedRes, res)

			// Assert the keystores
			for k, v := range tt.expectedKeystores {
				assert.Equal(t, v.messages, p.storage[k].messages)
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
		messages     []MsgLog
		initialLogs  map[string][]gossipLog
		expectedLogs map[string][]gossipLog
	}{
		{
			name:        "Add first log",
			node:        "n1",
			key:         "k1",
			id:          "id111",
			messages:    []MsgLog{{1, 100}, {2, 200}, {3, 300}},
			initialLogs: map[string][]gossipLog{},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
				},
			},
		},
		{
			name:     "Append new log",
			node:     "n1",
			key:      "k1",
			id:       "id222",
			messages: []MsgLog{{4, 400}, {5, 500}, {6, 600}},
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
					{gossipId: "id222", key: "k1", messages: []MsgLog{{4, 400}, {5, 500}, {6, 600}}},
				},
			},
		},
		{
			name:     "Append duplicate offset",
			node:     "n1",
			key:      "k1",
			id:       "id222",
			messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}},
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
					{gossipId: "id222", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
				},
			},
		},
		{
			name:     "Append with different key",
			node:     "n1",
			key:      "k2",
			id:       "id222",
			messages: []MsgLog{{11, 1100}, {12, 1200}, {13, 1300}},
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "id111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
					{gossipId: "id222", key: "k2", messages: []MsgLog{{11, 1100}, {12, 1200}, {13, 1300}}},
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

			p.addGossipLog(tt.node, tt.id, tt.key, tt.messages)

			assert.Empty(
				t,
				cmp.Diff(
					tt.expectedLogs,
					p.gossiped,
					cmp.AllowUnexported(gossipLog{}, MsgLog{}),
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
					{gossipId: "gossip111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
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
					{gossipId: "gossip111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
				},
			},
		},
		{
			name: "Ack one from many logs same node",
			src:  "n1",
			id:   "gossip222",
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
					{gossipId: "gossip222", key: "k2", messages: []MsgLog{{4, 44}, {5, 55}, {6, 66}}},
					{gossipId: "gossip333", key: "k5", messages: []MsgLog{{40, 400}, {50, 500}, {60, 600}}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
					{gossipId: "gossip333", key: "k5", messages: []MsgLog{{40, 400}, {50, 500}, {60, 600}}},
				},
			},
		},
		{
			name: "Ack one from many logs diff nodes",
			src:  "n2",
			id:   "gossip222",
			initialLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
				},
				"n2": {
					{gossipId: "gossip222", key: "k2", messages: []MsgLog{{4, 44}, {5, 55}, {6, 66}}},
					{gossipId: "gossip333", key: "k5", messages: []MsgLog{{40, 400}, {50, 500}, {60, 600}}},
				},
			},
			expectedLogs: map[string][]gossipLog{
				"n1": {
					{gossipId: "gossip111", key: "k1", messages: []MsgLog{{1, 100}, {2, 200}, {3, 300}}},
				},
				"n2": {
					{gossipId: "gossip333", key: "k5", messages: []MsgLog{{40, 400}, {50, 500}, {60, 600}}},
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
					cmp.AllowUnexported(gossipLog{}, MsgLog{}),
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
		initialBuffer    map[string]msg_log_list
		gossiped         map[string][]gossipLog
		expectedGossiped map[string][]gossipLog
		expectedBuffer   map[string]msg_log_list
		mockNode         *MockNode
	}{
		{
			name:      "Empty buffer",
			node:      "n1",
			lastFlush: time.Now().Add(-1 * bufferAge),
			initialBuffer: map[string]msg_log_list{
				"n2": {},
			},
			gossiped:         map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{},
			expectedBuffer: map[string]msg_log_list{
				"n2": {},
			},
			mockNode: NewMockNode("n1"),
		},
		{
			name:      "Single node, single key",
			node:      "n1",
			lastFlush: time.Now().Add(-1 * bufferAge),
			initialBuffer: map[string]msg_log_list{
				"n2": {
					"k1": {{42, 1}},
				},
			},
			gossiped: map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{
				"n2": {
					{key: "k1", messages: []MsgLog{{42, 1}}},
				},
			},
			expectedBuffer: map[string]msg_log_list{
				"n2": {},
			},
			mockNode: NewMockNode("n1"),
		},
		{
			name:      "Single node, multiple keys",
			node:      "n1",
			lastFlush: time.Now().Add(-10 * bufferAge),
			initialBuffer: map[string]msg_log_list{
				"n2": {
					"k1": {{42, 1}},
					"k2": {{101, 2}},
				},
			},
			gossiped: map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{
				"n2": {
					{key: "k1", messages: []MsgLog{{42, 1}}},
					{key: "k2", messages: []MsgLog{{101, 2}}},
				},
			},
			expectedBuffer: map[string]msg_log_list{
				"n2": {},
			},
			mockNode: NewMockNode("n1"),
		},
		{
			name:      "Multiple nodes, multiple keys",
			node:      "n1",
			lastFlush: time.Now().Add(-10 * bufferAge), // needs a bit more time
			initialBuffer: map[string]msg_log_list{
				"n2": {
					"k1": []MsgLog{{42, 420}, {43, 430}},
					"k2": []MsgLog{{101, 2}},
				},
				"n3": {
					"k1": []MsgLog{{42, 420}, {44, 440}},
					"k3": []MsgLog{{202, 3}},
				},
			},
			gossiped: map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{
				"n2": {
					{key: "k1", messages: []MsgLog{{42, 420}, {43, 430}}},
					{key: "k2", messages: []MsgLog{{101, 2}}},
				},
				"n3": {
					{key: "k1", messages: []MsgLog{{42, 420}, {44, 440}}},
					{key: "k3", messages: []MsgLog{{202, 3}}},
				},
			},
			expectedBuffer: map[string]msg_log_list{
				"n2": {},
				"n3": {},
			},
			mockNode: NewMockNode("n1"),
		},
		{
			name:      "No flush",
			node:      "n1",
			lastFlush: time.Now(),
			initialBuffer: map[string]msg_log_list{
				"n2": {
					"k1": []MsgLog{{42, 420}},
				},
			},
			gossiped:         map[string][]gossipLog{},
			expectedGossiped: map[string][]gossipLog{},
			expectedBuffer: map[string]msg_log_list{
				"n2": {
					"k1": []MsgLog{{42, 420}},
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
			if !cmp.Equal(p.buffer, tt.expectedBuffer, cmp.AllowUnexported(gossipLog{}, MsgLog{})) {
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
						cmp.AllowUnexported(gossipLog{}, MsgLog{}),
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
					{gossipId: "g1", at: old, key: "k1", messages: []MsgLog{{42, 1}}},
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
					{gossipId: "g1", at: old, key: "k1", messages: []MsgLog{{42, 1}}},
					{gossipId: "g2", at: old, key: "k2", messages: []MsgLog{{101, 1}}},
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
					{gossipId: "g1", at: old, key: "k1", messages: []MsgLog{{42, 1}}},
				},
				"n3": {
					{gossipId: "g2", at: old, key: "k2", messages: []MsgLog{{101, 1}}},
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
