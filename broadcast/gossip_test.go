package broadcast

import (
	"sync"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
)

func TestGossip(t *testing.T) {
	n := maelstrom.NewNode()
	n.Init("n1", []string{})
	p := Program{
		node:     n,
		bufMtx:   &sync.Mutex{},
		buffer:   make(map[string][]int64),
		gossiped: make(map[string][]gossipLog),
		topo:     map[string][]string{"n1": {"n2", "n3"}},
	}
	msgs := []int64{1, 2, 3, 4, 5, 6, 7}

	p.gossip("n2", msgs)

	assert.Empty(t, p.buffer["n2"], "buffer should be empty for source node")
	assert.Equal(t, msgs, p.buffer["n3"], "buffer should be updated for node n3")
}
