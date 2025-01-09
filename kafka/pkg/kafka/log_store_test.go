package kafka

import (
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyStore(t *testing.T) {
	tests := []struct {
		name            string
		initialMessages map[int64]int64
		committed       int64
		writeMessages   []MsgLog // Messages to write during the test
		readOffset      int64    // Offset to read from
		expectRead      []MsgLog // Expected result from `getMessages`
		commit          int64    // Offset to commit during the test
		expectCommitted int64    // Expected committed offset after commit
	}{
		{
			name:            "Empty store read",
			initialMessages: nil,
			writeMessages:   nil,
			readOffset:      1,
			expectRead:      []MsgLog{},
			commit:          0,
			expectCommitted: 0,
		},
		{
			name:            "Read from offset 2",
			initialMessages: map[int64]int64{1: 10, 2: 20, 3: 30},
			writeMessages:   nil,
			readOffset:      2,
			expectRead: []MsgLog{
				{Offset: 2, Msg: 20},
				{Offset: 3, Msg: 30},
			},
			commit:          2,
			expectCommitted: 2,
		},
		{
			name:            "Write and read from offset 3",
			initialMessages: map[int64]int64{1: 10},
			writeMessages: []MsgLog{
				{Offset: 2, Msg: 20},
				{Offset: 3, Msg: 30},
			},
			readOffset: 3,
			expectRead: []MsgLog{
				{Offset: 3, Msg: 30},
			},
			commit:          3,
			expectCommitted: 3,
		},
		{
			name:            "Read monotonous offsets only",
			initialMessages: map[int64]int64{1: 10, 2: 20, 3: 30, 4: 40, 7: 70, 8: 80, 9: 90},
			writeMessages:   []MsgLog{},
			readOffset:      2,
			expectRead: []MsgLog{
				{Offset: 2, Msg: 20},
				{Offset: 3, Msg: 30},
				{Offset: 4, Msg: 40},
			},
			commit:          3,
			expectCommitted: 3,
		},
		{
			name:            "Read monotonous offsets missing asked offset",
			initialMessages: map[int64]int64{1: 10, 2: 20, 3: 30, 4: 40, 7: 70, 8: 80, 9: 90},
			writeMessages:   []MsgLog{},
			readOffset:      6,
			expectRead:      []MsgLog{},
			commit:          3,
			expectCommitted: 3,
		},
		{
			name:            "Read monotonous offsets having asked offset",
			initialMessages: map[int64]int64{1: 10, 2: 20, 3: 30, 4: 40, 6: 60, 7: 70, 8: 80, 9: 90},
			writeMessages:   []MsgLog{},
			readOffset:      6,
			expectRead: []MsgLog{
				{Offset: 6, Msg: 60},
				{Offset: 7, Msg: 70},
				{Offset: 8, Msg: 80},
				{Offset: 9, Msg: 90},
			},
			commit:          3,
			expectCommitted: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			store := keyStore{
				key:       "testkey",
				committed: tt.committed,
				offsets:   slices.Sorted(maps.Keys(tt.initialMessages)),
				messages:  tt.initialMessages,
				mtx:       &sync.RWMutex{},
			}

			for _, m := range tt.writeMessages {
				store.store(m)
			}
			// t.Logf("logs %+v", store)

			got := store.getMessages(tt.readOffset)
			assert.Equal(t, tt.expectRead, got, "messages should match")

			store.commitOffset(tt.commit)
			assert.Equal(t, tt.expectCommitted, store.getCommittedOffset(), "committed offset should match")
		})
	}
}
