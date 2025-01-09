package kafka

import (
	"context"
	"sync"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
)

func TestGetNextOffset(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		initialOffset  int
		expectedOffset int64
		kvReadErr      error
		kvCasErr       error
		casCalledTimes int
	}{
		{
			name:           "initial offset 0",
			key:            "k1",
			initialOffset:  0,
			expectedOffset: 1,
			kvCasErr:       nil,
			casCalledTimes: 1,
		},
		{
			name:           "initial offset 5",
			key:            "k1",
			initialOffset:  5,
			expectedOffset: 6,
			kvCasErr:       nil,
			casCalledTimes: 1,
		},
		{
			name:           "non-precondition failure, error returned",
			key:            "k1",
			initialOffset:  0,
			expectedOffset: 1,
			kvCasErr:       maelstrom.NewRPCError(maelstrom.KeyDoesNotExist, "KV error"),
			casCalledTimes: 1,
		},
		{
			name:           "cas failure, should retry",
			key:            "k1",
			initialOffset:  5,
			expectedOffset: 6,
			kvCasErr:       maelstrom.NewRPCError(maelstrom.PreconditionFailed, "Cas error"),
			casCalledTimes: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLinKV := NewMockKV()
			mockLinKV.ReturnValueInt = tt.initialOffset
			mockLinKV.ReturnReadError = tt.kvReadErr
			mockLinKV.ReturnCasError = tt.kvCasErr

			mockSeqKV := NewMockKV()
			mockNode := NewMockNode("n1")
			p := New(mockNode, mockLinKV, mockSeqKV)

			offset := p.getNextOffset(tt.key)
			assert.Equal(t, tt.expectedOffset, offset)
			assert.Equal(t, tt.casCalledTimes, mockLinKV.CasCalled)
		})
	}
}

func TestStoreMsg(t *testing.T) {
	tests := []struct {
		name            string
		key             string
		entry           msgLog
		initialStorage  map[string]*keyStore
		expectedOffsets []int64
		kvWriteError    error
	}{
		{
			name:            "new key",
			key:             "k1",
			entry:           msgLog{offset: 1, msg: 100},
			initialStorage:  map[string]*keyStore{},
			expectedOffsets: []int64{1},
		},
		{
			name:  "existing key",
			key:   "k1",
			entry: msgLog{offset: 2, msg: 200},
			initialStorage: map[string]*keyStore{
				"k1": {
					key:       "k1",
					committed: 0,
					mtx:       &sync.RWMutex{},
					offsets:   []int64{1},
				},
			},
			expectedOffsets: []int64{1, 2},
		},
		{
			name:            "kv write error",
			key:             "k1",
			entry:           msgLog{offset: 1, msg: 100},
			initialStorage:  map[string]*keyStore{},
			expectedOffsets: []int64{1}, // offset is still stored locally
			kvWriteError:    context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLinKV := NewMockKV()
			mockSeqKV := NewMockKV()
			mockNode := NewMockNode("n1")
			p := New(mockNode, mockLinKV, mockSeqKV)

			p.storage = tt.initialStorage
			mockLinKV.ReturnWriteError = tt.kvWriteError

			p.storeMsg(tt.key, tt.entry)

			ks, ok := p.storage[tt.key]
			if !ok && len(tt.expectedOffsets) > 0 {
				t.Errorf("key %s not found in storage", tt.key)
			}

			if ok {
				ks.mtx.RLock()
				assert.Equal(t, tt.expectedOffsets, ks.offsets)
				ks.mtx.RUnlock()
			}

			if tt.kvWriteError != nil {
				assert.Equal(t, formatMsgKey(tt.key, tt.entry.offset), mockSeqKV.LastKey)
				assert.Equal(t, tt.entry.msg, mockSeqKV.LastValue)
				assert.Equal(t, 1, mockSeqKV.WriteCalled)

			}

		})
	}
}

func TestHandleInit(t *testing.T) {
	tests := []struct {
		name         string
		node         string
		nodeIDs      []string
		expectedTopo []string
	}{
		{
			name:         "Single node",
			node:         "n1",
			nodeIDs:      []string{"n1"},
			expectedTopo: []string{},
		},
		{
			name:         "Multiple nodes",
			node:         "n1",
			nodeIDs:      []string{"n1", "n2", "n3"},
			expectedTopo: []string{"n2", "n3"},
		},
		{
			name:         "Node with itself",
			node:         "n1",
			nodeIDs:      []string{"n1", "n1", "n2"},
			expectedTopo: []string{"n2"},
		},
		{
			name:         "Empty node list",
			node:         "n1",
			nodeIDs:      []string{},
			expectedTopo: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockNode := &MockNode{
				NodeID:     tt.node,
				NodeIdList: tt.nodeIDs,
			}
			p := Program{
				node: mockNode,
				topo: []string{},
			}

			resp := p.handleInit()

			assert.Equal(t, "init_ok", resp.Type)
			assert.Equal(t, tt.expectedTopo, p.topo)

		})
	}
}

func TestHandleSend(t *testing.T) {
	tests := []struct {
		name           string
		body           workload
		initialOffset  int
		expectedOffset int64
		expectedMsg    int64
		kvCasError     error
	}{
		{
			name:           "basic send",
			body:           workload{MessageBody: maelstrom.MessageBody{}, Type: "send", Key: "k1", Msg: 123},
			initialOffset:  0,
			expectedOffset: 1,
			expectedMsg:    123,
		},
		{
			name:           "send with existing offset",
			body:           workload{MessageBody: maelstrom.MessageBody{}, Type: "send", Key: "k2", Msg: 456},
			initialOffset:  5,
			expectedOffset: 6,
			expectedMsg:    456,
		},
		{
			name:           "cas error during offset increment",
			body:           workload{MessageBody: maelstrom.MessageBody{}, Type: "send", Key: "k3", Msg: 789},
			initialOffset:  10,
			kvCasError:     maelstrom.NewRPCError(maelstrom.PreconditionFailed, "CAS error"), // Will retry and eventually succeed in the mock
			expectedOffset: 11,                                                               // Should still get the next offset eventually
			expectedMsg:    789,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLinKV := NewMockKV()
			mockLinKV.ReturnValueInt = tt.initialOffset
			mockLinKV.ReturnCasError = tt.kvCasError
			mockSeqKV := NewMockKV()
			mockNode := NewMockNode("n1")

			// p := New(mockNode, mockLinKV, mockSeqKV)
			p := Program{
				node:        mockNode,
				linKv:       mockLinKV,
				seqKv:       mockSeqKV,
				storageChan: make(chan map[string]msgLog, 5),
			}
			resp := p.handleSend(tt.body)

			assert.Equal(t, "send_ok", resp.Type)
			assert.Equal(t, tt.expectedOffset, resp.Offset)

			// Check if the message was sent to the storage channel
			select {
			case storedMsgs := <-p.storageChan:
				assert.Contains(t, storedMsgs, tt.body.Key)
				assert.Equal(t, tt.expectedOffset, storedMsgs[tt.body.Key].offset)
				assert.Equal(t, tt.expectedMsg, storedMsgs[tt.body.Key].msg)
			default:
				t.Fatal("Message not sent to storage channel")
			}

		})
	}
}

func TestHandlePoll(t *testing.T) {
	tests := []struct {
		name         string
		body         workload
		storage      map[string]*keyStore
		kvReadData   map[string]any
		kvReadError  error
		expectedMsgs message_list
	}{
		{
			name: "no messages",
			body: workload{Offsets: map[string]int64{"k1": 0}},
			storage: map[string]*keyStore{
				"k1": NewKeyStore("k1"), // Empty keystore
			},
			expectedMsgs: message_list{"k1": []offset_msg_pair{}},
		},
		{
			name: "single message",
			body: workload{Offsets: map[string]int64{"k1": 0}},
			storage: map[string]*keyStore{
				"k1": {
					key:       "k1",
					committed: 0,
					mtx:       &sync.RWMutex{},
					offsets:   []int64{1},
				},
			},
			kvReadData: map[string]any{
				formatMsgKey("k1", 1): 100,
			},
			expectedMsgs: message_list{
				"k1": []offset_msg_pair{{1, 100}},
			},
		},
		{
			name: "multiple messages, different offsets",
			body: workload{Offsets: map[string]int64{"k1": 2}},
			storage: map[string]*keyStore{
				"k1": {
					key:       "k1",
					committed: 0,
					mtx:       &sync.RWMutex{},
					offsets:   []int64{1, 2, 3},
				},
			},
			kvReadData: map[string]any{
				formatMsgKey("k1", 2): 200,
				formatMsgKey("k1", 3): 300,
			},
			expectedMsgs: message_list{
				"k1": []offset_msg_pair{{2, 200}, {3, 300}},
			},
		},
		{
			name: "multiple keys",
			body: workload{Offsets: map[string]int64{"k1": 0, "k2": 0}},
			storage: map[string]*keyStore{
				"k1": {
					key:       "k1",
					committed: 0,
					mtx:       &sync.RWMutex{},
					offsets:   []int64{1},
				},
				"k2": {
					key:       "k2",
					committed: 0,
					mtx:       &sync.RWMutex{},
					offsets:   []int64{2},
				},
			},
			kvReadData: map[string]any{
				formatMsgKey("k1", 1): 100,
				formatMsgKey("k2", 2): 200,
			},
			expectedMsgs: message_list{
				"k1": []offset_msg_pair{{1, 100}},
				"k2": []offset_msg_pair{}, // no poll returned because of gap in offsets
			},
		},
		{
			name: "kv read error",
			body: workload{Offsets: map[string]int64{"k1": 0}},
			storage: map[string]*keyStore{
				"k1": {
					key:       "k1",
					committed: 0,
					mtx:       &sync.RWMutex{},
					offsets:   []int64{1},
				},
			},
			kvReadError:  maelstrom.NewRPCError(maelstrom.KeyDoesNotExist, "Read error"),
			expectedMsgs: message_list{"k1": []offset_msg_pair{}},
		},
		{
			name: "many multiple keys very long",
			body: workload{Offsets: map[string]int64{"k1": 3, "k2": 22, "k3": 50}},
			storage: map[string]*keyStore{
				"k1": {
					key:     "k1",
					mtx:     &sync.RWMutex{},
					offsets: getOffsets(1, 16),
				},
				"k2": {
					key:     "k2",
					mtx:     &sync.RWMutex{},
					offsets: getOffsets(10, 40),
				},
				"k3": {
					key:     "k3",
					mtx:     &sync.RWMutex{},
					offsets: getOffsets(1, 100),
				},
			},
			kvReadData: func() map[string]any {
				data := make(map[string]any)
				getKvReadData(&data, "k1", 1, 16)
				getKvReadData(&data, "k2", 10, 40)
				getKvReadData(&data, "k3", 1, 100)
				return data
			}(),
			expectedMsgs: message_list{
				"k1": getMessages(3, 3+MAX_POLL_LIST_LENGTH),
				"k2": getMessages(22, 22+MAX_POLL_LIST_LENGTH),
				"k3": getMessages(50, 50+MAX_POLL_LIST_LENGTH),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLinKV := NewMockKV()
			mockSeqKV := NewMockKV()
			mockSeqKV.ReturnValues = tt.kvReadData
			mockSeqKV.ReturnReadError = tt.kvReadError

			mockNode := NewMockNode("n1")
			p := New(mockNode, mockLinKV, mockSeqKV)
			p.storage = tt.storage

			resp := p.handlePoll(tt.body)

			assert.Equal(t, "poll_ok", resp.Type)
			for k, m := range resp.Msgs {
				assert.LessOrEqualf(t, len(m), MAX_POLL_LIST_LENGTH, "list size %d for poll response on %s exceeds set limit of %d (via const MAX_POLL_LIST_LENGTH)", len(m), k, MAX_POLL_LIST_LENGTH)
			}
			assert.Equal(t, tt.expectedMsgs, resp.Msgs)
		})
	}
}

func TestHandleCommitOffsets(t *testing.T) {
	tests := []struct {
		name              string
		body              workload
		initialStorage    map[string]*keyStore
		initialCommitted  map[string]int
		expectedCommitted map[string]int64
		kvCasError        error
		expectedCasCalls  int
	}{
		{
			name: "commit single offset",
			body: workload{Offsets: map[string]int64{"k1": 10}},
			initialStorage: map[string]*keyStore{
				"k1": {key: "k1", committed: 5, mtx: &sync.RWMutex{}},
			},
			initialCommitted:  map[string]int{formatCommittedOffsetKey("k1"): 5},
			expectedCommitted: map[string]int64{"k1": 10},
			expectedCasCalls:  1,
		},
		{
			name: "commit multiple offsets",
			body: workload{Offsets: map[string]int64{"k1": 10, "k2": 20}},
			initialStorage: map[string]*keyStore{
				"k1": {key: "k1", committed: 5, mtx: &sync.RWMutex{}},
				"k2": {key: "k2", committed: 15, mtx: &sync.RWMutex{}},
			},
			initialCommitted: map[string]int{
				formatCommittedOffsetKey("k1"): 5,
				formatCommittedOffsetKey("k2"): 15,
			},
			expectedCommitted: map[string]int64{"k1": 10, "k2": 20},
			expectedCasCalls:  2,
		},
		{
			name: "commit offset for non-existent key",
			body: workload{Offsets: map[string]int64{"k3": 30}},
			initialStorage: map[string]*keyStore{
				"k1": {key: "k1", committed: 5, mtx: &sync.RWMutex{}},
				"k2": {key: "k2", committed: 15, mtx: &sync.RWMutex{}},
			},
			initialCommitted: map[string]int{
				formatCommittedOffsetKey("k1"): 5,
				formatCommittedOffsetKey("k2"): 15,
			},
			expectedCommitted: map[string]int64{"k1": 5, "k2": 15, "k3": 30},
			expectedCasCalls:  1,
		},
		{
			name: "cas error",
			body: workload{Offsets: map[string]int64{"k1": 10}},
			initialStorage: map[string]*keyStore{
				"k1": {key: "k1", committed: 5, mtx: &sync.RWMutex{}},
			},
			initialCommitted:  map[string]int{formatCommittedOffsetKey("k1"): 5},
			kvCasError:        maelstrom.NewRPCError(maelstrom.PreconditionFailed, "CAS error"),
			expectedCommitted: map[string]int64{"k1": 10}, //Local storage is updated regardless
			expectedCasCalls:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLinKV := NewMockKV()
			mockSeqKV := NewMockKV()
			mockNode := NewMockNode("n1")
			p := New(mockNode, mockLinKV, mockSeqKV)

			for k, v := range tt.initialCommitted {
				mockLinKV.ReturnValues[k] = v
			}
			mockLinKV.ReturnCasError = tt.kvCasError

			p.storage = tt.initialStorage

			resp := p.handleCommitOffsets(tt.body)

			assert.Equal(t, "commit_offsets_ok", resp.Type)

			for k, expectedOffset := range tt.expectedCommitted {
				ks, ok := p.storage[k]
				assert.Truef(t, ok, "key (%s) should exist in local storage: %+v", k, p.storage)
				if ok {
					assert.Equal(t, expectedOffset, ks.committed)
				}

			}
			assert.Equal(t, tt.expectedCasCalls, mockLinKV.CasCalled)

		})
	}
}

func TestHandleListCommittedOffsets(t *testing.T) {
	tests := []struct {
		name            string
		body            workload
		kvReturnValues  map[string]int
		kvReadError     error
		expectedOffsets key_offset_pair
	}{
		{
			name:            "list single offset",
			body:            workload{Keys: []string{"k1"}},
			kvReturnValues:  map[string]int{formatCommittedOffsetKey("k1"): 10},
			expectedOffsets: key_offset_pair{"k1": 10},
		},
		{
			name:            "list multiple offsets",
			body:            workload{Keys: []string{"k1", "k2"}},
			kvReturnValues:  map[string]int{formatCommittedOffsetKey("k1"): 10, formatCommittedOffsetKey("k2"): 20},
			expectedOffsets: key_offset_pair{"k1": 10, "k2": 20},
		},
		{
			name:            "list offset for non-existent key",
			body:            workload{Keys: []string{"k3"}},
			kvReturnValues:  map[string]int{formatCommittedOffsetKey("k1"): 10, formatCommittedOffsetKey("k2"): 20},
			expectedOffsets: key_offset_pair{"k3": 0}, // Expect 0 for non-existent keys
		},
		{
			name:            "kv read error",
			body:            workload{Keys: []string{"k1"}},
			kvReturnValues:  map[string]int{formatCommittedOffsetKey("k1"): 10},
			kvReadError:     context.Canceled,
			expectedOffsets: key_offset_pair{"k1": 0}, // Expect 0 on read error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLinKV := NewMockKV()
			mockSeqKV := NewMockKV()
			mockNode := NewMockNode("n1")

			for k, v := range tt.kvReturnValues {
				mockLinKV.ReturnValues[k] = v // or mockSeqKV if you use a seq kv
			}
			mockLinKV.ReturnReadError = tt.kvReadError
			p := New(mockNode, mockLinKV, mockSeqKV)

			resp := p.handleListCommittedOffsets(tt.body)

			assert.Equal(t, "list_committed_offsets_ok", resp.Type)
			assert.Equal(t, tt.expectedOffsets, resp.Offsets)
			assert.Equal(t, len(tt.body.Keys), mockLinKV.ReadIntCalled) // Assert the correct number of read calls

		})
	}
}

func TestGetKeyStore(t *testing.T) {
	tests := []struct {
		name             string
		key              string
		initialStorage   map[string]*keyStore
		expectedKeyStore *keyStore
	}{
		{
			name:             "Key exists",
			key:              "k1",
			initialStorage:   map[string]*keyStore{"k1": {key: "k1", committed: 0, mtx: &sync.RWMutex{}}},
			expectedKeyStore: &keyStore{key: "k1", committed: 0, mtx: &sync.RWMutex{}},
		},
		{
			name:             "Key does not exist",
			key:              "k2",
			initialStorage:   map[string]*keyStore{"k1": {key: "k1", committed: 0, mtx: &sync.RWMutex{}}},
			expectedKeyStore: &keyStore{key: "k2", committed: 0, mtx: &sync.RWMutex{}},
		},
		{
			name:             "Empty storage",
			key:              "k1",
			initialStorage:   map[string]*keyStore{},
			expectedKeyStore: &keyStore{key: "k1", committed: 0, mtx: &sync.RWMutex{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			mockLinKV := NewMockKV()
			mockSeqKV := NewMockKV()
			mockNode := NewMockNode("n1")
			p := New(mockNode, mockLinKV, mockSeqKV)

			p.storage = tt.initialStorage

			ks := p.getKeyStore(tt.key)

			// Don't compare mutexes directly.
			assert.NotNil(t, ks)
			assert.Equal(t, tt.expectedKeyStore.key, ks.key)
			assert.Equal(t, tt.expectedKeyStore.committed, ks.committed)
			assert.Contains(t, p.storage, tt.key)
			assert.Same(t, ks, p.storage[tt.key])
		})
	}
}

func getOffsets(start, end int) []int64 {
	data := make([]int64, 0)
	for i := start; i <= end; i++ {
		data = append(data, int64(i))
	}
	return data
}

func getKvReadData(data *map[string]any, key string, start, end int) {
	for i := start; i <= end; i++ {
		(*data)[formatMsgKey(key, int64(i))] = int(i + 100)
	}
}

func getMessages(start, end int) []offset_msg_pair {
	msgs := []offset_msg_pair{}
	for i := start; i < end; i++ {
		msgs = append(msgs, offset_msg_pair{int64(i), int64(i + 100)})
	}
	return msgs
}
