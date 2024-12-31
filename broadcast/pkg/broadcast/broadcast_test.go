package broadcast

import (
	"encoding/json"
	"sync"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
)

func TestBroadcast(t *testing.T) {
	node := maelstrom.NewNode()
	brd := New(node)

	msgs := []int64{1, 2, 3, 4, 5, 9, 8, 7, 6, 10}
	for _, m := range msgs {
		brd.broadcast(
			workload{
				Message: m,
			},
			maelstrom.Message{},
		)
	}
	assert.Equal(t, msgs, brd.messages, "didn't get all of the broadcasted messages")
}

func TestBroadcastConcurrency(t *testing.T) {
	node := maelstrom.NewNode()
	brd := New(node)

	count := 10000
	workers := 100

	ch := make(chan int64, count)

	for i := 0; i < count; i++ {
		ch <- int64(i)
	}
	close(ch)

	var wg sync.WaitGroup
	wg.Add(workers)

	for j := 0; j < workers; j++ {
		go func() {
			defer wg.Done()
			for m := range ch {
				brd.broadcast(
					workload{
						Message: m,
					},
					maelstrom.Message{},
				)
			}
		}()
	}

	wg.Wait()

	messages := len(brd.messages)
	assert.Equal(t, count, messages, "lost %d messages in %d total", count-messages, count)
}

func TestRead(t *testing.T) {
	node := maelstrom.NewNode()
	brd := New(node)

	msgs := []int64{1, 2, 3, 4, 5, 9, 8, 7, 6, 10}
	for _, m := range msgs {
		brd.broadcast(
			workload{
				Message: m,
			},
			maelstrom.Message{},
		)
	}
	resp := brd.read(workload{})
	assert.Equal(t, msgs, resp["messages"], "didn't read all of the broadcasted messages")
}

func TestTopology(t *testing.T) {
	node := maelstrom.NewNode()
	brd := New(node)

	w := workload{
		Topology: map[string][]string{
			"n1": {"n2", "n3"},
			"n2": {"n3"},
			"n3": {"n1"},
		},
	}
	brd.topology(w)
	assert.Equal(t, w.Topology, brd.topo, "topology not saved correctly")
}

func TestGetHandle(t *testing.T) {
	node := maelstrom.NewNode()
	node.Init("n1", []string{"n1"})
	brd := New(node)

	w := workload{
		Type: "topology",
		Topology: map[string][]string{
			"n1": {"n2", "n3"},
			"n2": {"n3"},
			"n3": {"n1"},
		},
	}
	b, _ := json.Marshal(w)
	tp := brd.GetHandle("topology")
	res := tp(maelstrom.Message{
		Src:  "n1",
		Dest: "n2",
		Body: b,
	})
	assert.Nil(t, res, "error on topology workload")
	assert.Equal(t, w.Topology, brd.topo, "topology not saved correctly")
}

func TestRemoveFunction(t *testing.T) {
	list := []sentLog{
		{msg: int64(1)},
		{msg: int64(2)},
		{msg: int64(3)},
		{msg: int64(4)},
		{msg: int64(5)},
	}
	expected := []sentLog{
		{msg: int64(1)},
		{msg: int64(2)},
		{msg: int64(4)},
		{msg: int64(5)},
	}

	result := remove(list, 2)
	assert.ElementsMatch(t, expected, result)
}
