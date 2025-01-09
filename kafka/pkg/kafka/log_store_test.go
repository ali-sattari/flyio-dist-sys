package kafka

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyStore(t *testing.T) {
	tests := []struct {
		name            string
		initial         []int64
		committed       int64
		writeOffsets    []int64 // Offsets to write during the test
		readOffset      int64   // Offset to read from
		expectRead      []int64 // Expected result from `read`
		commit          int64   // Offset to commit during the test
		expectCommitted int64   // Expected committed offset after commit
	}{
		{
			name:            "Empty store read",
			initial:         nil,
			writeOffsets:    nil,
			readOffset:      1,
			expectRead:      []int64{},
			commit:          0,
			expectCommitted: 0,
		},
		{
			name:            "Read from offset 2",
			initial:         []int64{1, 2, 3},
			writeOffsets:    nil,
			readOffset:      2,
			expectRead:      []int64{2, 3},
			commit:          2,
			expectCommitted: 2,
		},
		{
			name:            "Write and read from offset 3",
			initial:         []int64{1},
			writeOffsets:    []int64{2, 3},
			readOffset:      3,
			expectRead:      []int64{3},
			commit:          3,
			expectCommitted: 3,
		},
		{
			name:            "Read monotonous offsets only",
			initial:         []int64{1, 2, 3, 4, 7, 8, 9},
			writeOffsets:    []int64{},
			readOffset:      2,
			expectRead:      []int64{2, 3, 4},
			commit:          3,
			expectCommitted: 3,
		},
		{
			name:            "Read monotonous offsets missing asked offset",
			initial:         []int64{1, 2, 3, 4, 7, 8, 9},
			writeOffsets:    []int64{},
			readOffset:      6,
			expectRead:      []int64{},
			commit:          3,
			expectCommitted: 3,
		},
		{
			name:            "Read monotonous offsets having asked offset",
			initial:         []int64{1, 2, 3, 4, 6, 7, 8, 9},
			writeOffsets:    []int64{},
			readOffset:      6,
			expectRead:      []int64{6, 7, 8, 9},
			commit:          3,
			expectCommitted: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := keyStore{
				key:       "testkey",
				committed: tt.committed,
				offsets:   tt.initial,
				mtx:       &sync.RWMutex{},
			}

			for _, o := range tt.writeOffsets {
				store.store(o)
			}
			// t.Logf("logs %+v", store)

			var got []int64
			got = store.getOffsets(tt.readOffset)

			assert.Equal(t, tt.expectRead, got, "offsets should match")

			store.commitOffset(tt.commit)
			assert.Equal(t, tt.expectCommitted, store.getCommittedOffset(), "committed offset should match")
		})
	}
}
