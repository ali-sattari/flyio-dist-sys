package broadcast

import (
	"sync"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
)

func TestBroadcast(t *testing.T) {
	node := maelstrom.Node{}
	brd := New(&node)

	msgs := []float64{1, 2, 3, 4, 5, 9, 8, 7, 6, 10}
	for _, m := range msgs {
		brd.broadcast(map[string]any{
			"message": m,
		})
	}
	assert.Equal(t, msgs, brd.messages, "didn't get all of the broadcasted messages")
}

var wg sync.WaitGroup

func TestBroadcastConcurrency(t *testing.T) {
	node := maelstrom.Node{}
	brd := New(&node)

	count := 10000
	ch := make(chan float64, count)
	wg.Add(count)
	defer close(ch)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			brd.broadcast(map[string]any{
				"message": float64(i),
			})
		}()
	}
	wg.Wait()

	messages := len(brd.messages)
	assert.Equal(t, count, messages, "lost %d mesages in %d total", count-messages, count)
}

func TestRead(t *testing.T) {
	node := maelstrom.Node{}
	brd := New(&node)

	msgs := []float64{1, 2, 3, 4, 5, 9, 8, 7, 6, 10}
	for _, m := range msgs {
		brd.broadcast(map[string]any{
			"message": m,
		})
	}
	resp := brd.read(map[string]any{})
	assert.Equal(t, msgs, resp["messages"], "didn't read all of the broadcasted messages")
}
