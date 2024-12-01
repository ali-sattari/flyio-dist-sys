package broadcast

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
)

func TestBroadcast(t *testing.T) {
	node := maelstrom.NewNode()
	brd := New(node)

	msgs := []int64{1, 2, 3, 4, 5, 9, 8, 7, 6, 10}
	for _, m := range msgs {
		brd.broadcast(workload{
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
				brd.broadcast(workload{
					Message: m,
				})
			}
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
		brd.broadcast(workload{
			Message: m,
		})
	}
	resp := brd.read(workload{})
	assert.Equal(t, msgs, resp["messages"], "didn't read all of the broadcasted messages")
}

func TestTopology(t *testing.T) {
	node := maelstrom.NewNode()
	brd := New(node)

	w := workload{
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

	w := workload{
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

func TestAddSentLog(t *testing.T) {
	node := maelstrom.NewNode()
	node.Init("n1", []string{"n1"})
	p := New(node)
	id := 12345
	msg := int64(67890)

	// Test when the map is empty
	p.addSentLog(node.ID(), id, msg)
	assert.Len(t, p.sent[node.ID()], 1)

	// Test when the map is not empty
	p.sent[node.ID()] = []sentLog{{at: time.Now().UnixMicro() - 1000, msgId: id, msg: msg}}
	p.addSentLog(node.ID(), id+1, msg)
	assert.Len(t, p.sent[node.ID()], 2)
}

func TestRemoveFunction(t *testing.T) {
	list := []sentLog{
		sentLog{msg: int64(1)},
		sentLog{msg: int64(2)},
		sentLog{msg: int64(3)},
		sentLog{msg: int64(4)},
		sentLog{msg: int64(5)},
	}
	expected := []sentLog{
		sentLog{msg: int64(1)},
		sentLog{msg: int64(2)},
		sentLog{msg: int64(4)},
		sentLog{msg: int64(5)},
	}

	result := remove(list, 2)
	assert.ElementsMatch(t, expected, result)
}
