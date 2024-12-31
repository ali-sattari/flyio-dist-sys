package snowflaker

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	t.Run("valid node ID", func(t *testing.T) {
		node, ok := New(1)
		assert.NoError(t, ok, "No error on valid ID: %+v", ok)
		assert.IsType(t, Generator{}, node, "Return valid type")
	})

	t.Run("invalid node ID", func(t *testing.T) {
		_, ok := New(math.MaxUint16)
		assert.Error(t, ok, "Error on invalid (large) node ID")
	})
}

var wg sync.WaitGroup

func TestGetenerateID(t *testing.T) {
	node, _ := New(1)

	count := 10000
	ch := make(chan int64, count)
	wg.Add(count)
	defer close(ch)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			id := node.GenerateID()
			ch <- id
		}()
	}
	wg.Wait()

	m := make(map[int64]int)
	for i := 0; i < count; i++ {
		id := <-ch
		m[id]++
	}
	uids := len(m)
	assert.Equal(t, count, uids, "%d duplicates found amond %d IDs", count-uids, count)
}
