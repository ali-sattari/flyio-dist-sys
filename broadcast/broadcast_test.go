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
		brd.broadcast(Workload{
			Message: m,
		})
	}
	assert.Equal(t, msgs, brd.messages, "didn't get all of the broadcasted messages")
}

var wg sync.WaitGroup

func TestBroadcastConcurrency(t *testing.T) {
	node := maelstrom.NewNode()
	brd := New(node)

	count := 10000
	ch := make(chan float64, count)
	wg.Add(count)
	defer close(ch)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			brd.broadcast(Workload{
				Message: int64(i),
			})
		}()
	}
	wg.Wait()

	messages := len(brd.messages)
	assert.Equal(t, count, messages, "lost %d mesages in %d total", count-messages, count)
}

func TestRead(t *testing.T) {
	node := maelstrom.NewNode()
	brd := New(node)

	msgs := []int64{1, 2, 3, 4, 5, 9, 8, 7, 6, 10}
	for _, m := range msgs {
		brd.broadcast(Workload{
			Message: m,
		})
	}
	resp := brd.read(Workload{})
	assert.Equal(t, msgs, resp["messages"], "didn't read all of the broadcasted messages")
}

func TestTopology(t *testing.T) {
	node := maelstrom.NewNode()
	brd := New(node)

	w := Workload{
		Topology: map[string][]string{
			"n1": []string{"n2", "n3"},
			"n2": []string{"n3"},
			"n3": []string{"n1"},
		},
	}
	brd.topology(w)
	assert.Equal(t, w.Topology, brd.topo, "topology not saved correctly")
}

func TestGetHandle(t *testing.T) {
	node := maelstrom.NewNode()
	node.Init("n1", []string{"n1"})
	brd := New(node)

	w := Workload{
		Type: "topology",
		Topology: map[string][]string{
			"n1": []string{"n2", "n3"},
			"n2": []string{"n3"},
			"n3": []string{"n1"},
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
