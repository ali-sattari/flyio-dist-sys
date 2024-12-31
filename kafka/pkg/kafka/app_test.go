package kafka

import (
	"encoding/json"
	"reflect"
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
				Offset: 101, // Assuming lastOffset starts from 100
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
				Offsets: map[string]int64{"testKey": 0},
			},
			expected: response{
				Type: "poll_ok",
				Msgs: map[string][]msgLog{},
			},
		},
		{
			name: "Single Message",
			key:  "testKey",
			body: workload{
				Type:    "poll",
				Offsets: map[string]int64{"testKey": 0},
			},
			expected: response{
				Type: "poll_ok",
				Msgs: map[string][]msgLog{"testKey": []msgLog{{offset: 101, msg: 100}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := maelstrom.NewNode()
			p := New(n)
			if tt.body.Type == "poll" && len(tt.expected.Msgs[tt.key]) > 0 {
				p.storage[tt.key] = keyStore{
					logs: []msgLog{{offset: 101, msg: 100}},
				}
			}

			resp := p.handlePoll(tt.body)
			if resp.Type != tt.expected.Type || !reflect.DeepEqual(resp.Msgs, tt.expected.Msgs) {
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
				Offsets: map[string]int64{"testKey": 101},
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
			p.storage[tt.key] = keyStore{
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
		expected response
	}{
		{
			name: "Single Key",
			body: workload{
				Type: "list_committed_offsets",
				Keys: []string{"testKey"},
			},
			expected: response{
				Type:    "list_committed_offsets_ok",
				Offsets: map[string]int64{"testKey": 101},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := maelstrom.NewNode()
			p := New(n)
			p.storage[tt.body.Keys[0]] = keyStore{
				logs:      []msgLog{{offset: 101, msg: 100}},
				committed: 101,
			}

			resp := p.handleListCommittedOffsets(tt.body)
			if resp.Type != tt.expected.Type || !reflect.DeepEqual(resp.Offsets, tt.expected.Offsets) {
				t.Errorf("expected response %+v, got %+v", tt.expected, resp)
			}
		})
	}
}
