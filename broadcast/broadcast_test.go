package broadcast

import (
	"fmt"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
)

func TestHandleBroadcast(t *testing.T) {
	node := maelstrom.Node{}
	brd := New(&node)

	fmt.Printf("%+v\n", brd)

	msgs := []float64{1, 2, 3, 4, 5, 9, 8, 7, 6}
	for _, m := range msgs {
		b := map[string]any{
			"message": m,
		}
		brd.broadcast(b)
	}
	assert.Equal(t, msgs, brd.messages, "didn't get all of the broadcasted messages")
}
