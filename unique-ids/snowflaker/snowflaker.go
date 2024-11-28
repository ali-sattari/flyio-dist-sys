package snowflaker

import (
	"fmt"
	"sync"
	"time"
)

const (
	nodeBits     = uint64(10)
	sequenceBits = uint64(12)

	maxNodeID   = int64(-1) ^ (int64(-1) << nodeBits) // The maximum value of the node ID used to prevent overflow
	maxSequence = int64(-1) ^ (int64(-1) << sequenceBits)

	timeShift = int64(nodeBits + sequenceBits)
)

type Generator struct {
	nodeid        int64
	sequence      int64
	lastTimeStamp int64
	mtx           sync.Mutex
}

type ID int64

func New(nId int64) (Generator, error) {
	if nId == 0 || nId > maxNodeID {
		return Generator{}, fmt.Errorf("invalid machine ID needs to be 10 bits uint: %d > %d", nId, maxNodeID)
	}

	return Generator{
		nodeid:        nId,
		sequence:      0,
		lastTimeStamp: getTimeMs(),
	}, nil
}

func getTimeMs() int64 {
	t := time.Now().UnixNano()
	return t
}

func (g *Generator) GenerateID() int64 {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	t := getTimeMs()

	// Handle clock drift backward
	if t < g.lastTimeStamp {
		t = g.lastTimeStamp
	}

	if t == g.lastTimeStamp {
		g.sequence++
		if g.sequence > maxSequence {
			// Wait for the next millisecond
			for t <= g.lastTimeStamp {
				t = getTimeMs()
			}
			g.sequence = 0
		}
	} else {
		// Reset sequence for a new millisecond
		g.sequence = 0
	}

	g.lastTimeStamp = t

	id := (t << timeShift) | (g.nodeid << nodeBits) | g.sequence

	return id
}
