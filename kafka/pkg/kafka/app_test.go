package kafka

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func TestHandleSend(t *testing.T) {
	tests := []struct {
		name     string
		body     workload
		expected response
	}{
		{
			name: "Single Message",
			body: workload{
				Type: "send",
				Key:  "testKey",
				Msg:  100,
			},
			expected: response{
				Type:   "send_ok",
				Offset: LAST_OFFSET_START + 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := maelstrom.NewNode()
			p := New(n)
			msg := maelstrom.Message{
				Body: json.RawMessage(`{}`),
			}
			if err := json.Unmarshal([]byte(`{"type": "send", "key": "testKey", "msg": 100}`), &msg.Body); err != nil {
				t.Fatalf("failed to unmarshal message body: %v", err)
			}

			resp := p.handleSend(tt.body)
			if resp.Type != tt.expected.Type || resp.Offset != tt.expected.Offset {
				t.Errorf("expected response %+v, got %+v", tt.expected, resp)
			}
		})
	}
}

func TestHandlePoll(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		body     workload
		expected response
	}{
		{
			name: "No Messages",
			key:  "testKey",
			body: workload{
				Type:    "poll",
				Offsets: offset_list{"testKey": 0},
			},
			expected: response{
				Type: "poll_ok",
				Msgs: message_list{"testKey": {}},
			},
		},
		{
			name: "Single Message",
			key:  "testKey",
			body: workload{
				Type:    "poll",
				Offsets: offset_list{"testKey": 0},
			},
			expected: response{
				Type: "poll_ok",
				Msgs: message_list{"testKey": {{101, 100}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := maelstrom.NewNode()
			p := New(n)
			if tt.body.Type == "poll" && len(tt.expected.Msgs[tt.key]) > 0 {
				for _, ml := range tt.expected.Msgs[tt.key] {
					p.storeMsg(tt.key, msgLog{ml[0], ml[1]})
				}
			}

			resp := p.handlePoll(tt.body)
			if resp.Type != tt.expected.Type || fmt.Sprint(resp.Msgs) != fmt.Sprint(tt.expected.Msgs) {
				t.Errorf("expected response %+v, got %+v", tt.expected, resp)
			}
		})
	}
}

func TestHandleCommitOffsets(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		body     workload
		expected response
	}{
		{
			name: "Successful Commit",
			key:  "testKey",
			body: workload{
				Type:    "commit_offsets",
				Offsets: offset_list{"testKey": 101},
			},
			expected: response{
				Type: "commit_offsets_ok",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := maelstrom.NewNode()
			p := New(n)
			p.storage[tt.key] = &keyStore{
				logs: []msgLog{{offset: 101, msg: 100}},
			}

			resp := p.handleCommitOffsets(tt.body)
			if resp.Type != tt.expected.Type {
				t.Errorf("expected response %+v, got %+v", tt.expected, resp)
			}
		})
	}
}

func TestHandleListCommittedOffsets(t *testing.T) {
	tests := []struct {
		name     string
		body     workload
		storage  map[string]*keyStore
		expected response
	}{
		{
			name: "Single Key",
			body: workload{
				Type: "list_committed_offsets",
				Keys: []string{"k1"},
			},
			storage: map[string]*keyStore{
				"k1": {
					key:       "k1",
					committed: 101,
					logs:      []msgLog{{offset: 101, msg: 100}},
				},
			},
			expected: response{
				Type:    "list_committed_offsets_ok",
				Offsets: offset_list{"k1": 101},
			},
		},
		{
			name: "Multiple Keys",
			body: workload{
				Type: "list_committed_offsets",
				Keys: []string{"k1", "k2", "k3"},
			},
			storage: map[string]*keyStore{
				"k1": {
					key:       "k1",
					committed: 101,
				},
				"k2": {
					key:       "k2",
					committed: 42,
				},
				"k3": {
					key:       "k3",
					committed: 350,
				},
			},
			expected: response{
				Type:    "list_committed_offsets_ok",
				Offsets: offset_list{"k1": 101, "k2": 42, "k3": 350},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := maelstrom.NewNode()
			p := New(n)
			p.storage = tt.storage

			resp := p.handleListCommittedOffsets(tt.body)
			if resp.Type != tt.expected.Type || !reflect.DeepEqual(resp.Offsets, tt.expected.Offsets) {
				t.Errorf("expected response %+v, got %+v", tt.expected, resp)
			}
		})
	}
}

func TestGetNewOffset(t *testing.T) {
	tests := []struct {
		name     string
		initial  int64
		expected int64
	}{
		{
			name:     "First Call",
			initial:  LAST_OFFSET_START,
			expected: LAST_OFFSET_START + 1,
		},
		{
			name:     "Subsequent Calls",
			initial:  200,
			expected: 201,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Program{
				lastOffset:    tt.initial,
				lastOffsetMtx: &sync.Mutex{},
			}
			offset := p.getNewOffset()
			if offset != tt.expected {
				t.Errorf("expected offset %d, got %d", tt.expected, offset)
			}
		})
	}
}

func TestStoreMsg(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		entry    msgLog
		expected map[string]*keyStore
	}{
		{
			name: "New Key",
			key:  "testKey1",
			entry: msgLog{
				offset: 101,
				msg:    100,
			},
			expected: map[string]*keyStore{
				"testKey1": {
					key:       "testKey1",
					committed: 0,
					logs:      []msgLog{{offset: 101, msg: 100}},
				},
			},
		},
		{
			name: "Existing Key",
			key:  "testKey2",
			entry: msgLog{
				offset: 201,
				msg:    200,
			},
			expected: map[string]*keyStore{
				"testKey2": {
					key:       "testKey2",
					committed: 0,
					logs:      []msgLog{{offset: 201, msg: 200}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(&maelstrom.Node{})
			p.storeMsg(tt.key, tt.entry)
			if len(p.storage) != 1 {
				t.Errorf("expected storage to contain 1 key, got %d", len(p.storage))
			}

			actualKeyStore := p.storage[tt.key]
			if actualKeyStore == nil || len(actualKeyStore.logs) != 1 ||
				actualKeyStore.logs[0].offset != tt.entry.offset || actualKeyStore.logs[0].msg != tt.entry.msg {
				t.Errorf("expected key store %+v, got %+v", tt.expected, p.storage)
			}
		})
	}

	// Concurrency test: multiple writes to the same key
	t.Run("Concurrency", func(t *testing.T) {
		const numMessages = 1000
		p := New(&maelstrom.Node{})

		var wg sync.WaitGroup
		wg.Add(numMessages)

		for i := 0; i < numMessages; i++ {
			go func(i int) {
				defer wg.Done()
				p.storeMsg("concurrentKey", msgLog{offset: int64(i), msg: int64(i)})
			}(i)
		}

		wg.Wait()

		keyStore, ok := p.storage["concurrentKey"]
		if !ok || len(keyStore.logs) != numMessages {
			t.Errorf("expected key store with %d logs for 'concurrentKey', got %+v", numMessages, keyStore)
		}
	})
}
