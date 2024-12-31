package kafka

import (
	"reflect"
	"testing"
)

func TestKeyStore(t *testing.T) {
	tests := []struct {
		name            string
		initial         []msgLog
		committed       int64
		writeMsgs       []msgLog // Messages to write during the test
		readOffset      int64    // Offset to read from
		expectRead      []msgLog // Expected result from `read`
		commit          int64    // Offset to commit during the test
		expectCommitted int64    // Expected committed offset after commit
	}{
		{
			name:            "Empty store read",
			initial:         nil,
			writeMsgs:       nil,
			readOffset:      1,
			expectRead:      []msgLog{},
			commit:          0,
			expectCommitted: 0,
		},
		{
			name:       "Read from offset 2",
			initial:    []msgLog{{msg: 100, offset: 1}, {msg: 200, offset: 2}, {msg: 300, offset: 3}},
			writeMsgs:  nil,
			readOffset: 2,
			expectRead: []msgLog{
				{msg: 200, offset: 2},
				{msg: 300, offset: 3},
			},
			commit:          2,
			expectCommitted: 2,
		},
		{
			name:       "Write and read from offset 3",
			initial:    []msgLog{{msg: 100, offset: 1}},
			writeMsgs:  []msgLog{{msg: 200, offset: 2}, {msg: 300, offset: 3}},
			readOffset: 3,
			expectRead: []msgLog{
				{msg: 300, offset: 3},
			},
			commit:          3,
			expectCommitted: 3,
		},
		{
			name:            "Commit older offset",
			initial:         nil,
			writeMsgs:       []msgLog{{msg: 100, offset: 1}},
			readOffset:      0,
			expectRead:      []msgLog{{msg: 100, offset: 1}},
			committed:       10,
			commit:          0,
			expectCommitted: -1, // This indicates an error expected
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &keyStore{
				key:       "testKey",
				logs:      tt.initial,
				committed: tt.committed,
			}

			for _, msg := range tt.writeMsgs {
				store.write(msg.msg, msg.offset)
			}

			var got []msgLog
			got = store.read(tt.readOffset)

			if !reflect.DeepEqual(got, tt.expectRead) {
				t.Errorf("expected %+v, got %+v", tt.expectRead, got)
			}

			err := store.commitOffset(tt.commit)
			if tt.expectCommitted == -1 {
				if err == nil {
					t.Errorf("expected an error when committing offset %d", tt.commit)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error when committing offset %d: %v", tt.commit, err)
				}
				if store.getCommittedOffset() != tt.expectCommitted {
					t.Errorf("expected committed offset to be %d, got %d", tt.expectCommitted, store.getCommittedOffset())
				}
			}
		})
	}
}
