package broadcast

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Program struct {
	node      *maelstrom.Node
	mtx       *sync.RWMutex
	messages  []int64
	topo      map[string][]string
	sent      map[string][]sentLog
	gossiped  map[string][]gossipLog
	ackMtx    *sync.Mutex
	buffer    map[string][]int64
	bufMtx    *sync.Mutex
	lastFlush time.Time
	flushMtx  *sync.Mutex
}

type workload struct {
	maelstrom.MessageBody
	Type        string
	Message     int64
	MessageList []int64 `json:"message_list,omitempty"`
	GossipId    string  `json:"gossip_id,omitempty"`
	Topology    map[string][]string
}

var wg sync.WaitGroup

func New(n *maelstrom.Node) Program {
	p := Program{
		node:      n,
		mtx:       &sync.RWMutex{},
		bufMtx:    &sync.Mutex{},
		buffer:    make(map[string][]int64),
		ackMtx:    &sync.Mutex{},
		gossiped:  make(map[string][]gossipLog),
		flushMtx:  &sync.Mutex{},
		lastFlush: time.Now(),
	}

	retrier := p.periodicJobs(1*time.Second, p.resendUnackedGossips)
	flusher := p.periodicJobs(100*time.Millisecond, p.flushGossipBuffer)

	log.Printf("new buffer: %+v", p.buffer)

	wg.Add(2)
	go retrier()
	go flusher()

	return p
}

func (p *Program) GetHandle(rpc_type string) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body workload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var resp map[string]any
		switch rpc_type {
		case "broadcast":
			resp = p.broadcast(body, msg)
		case "broadcast_ok":
			p.ackSentLog(msg.Src, body)
			return nil
		case "read":
			resp = p.read(body)
		case "topology":
			resp = p.topology(body)
		case "gossip":
			resp = p.receiveGossip(body, msg)
		case "gossip_ok":
			p.ackGossip(msg.Src, body)
			return nil
		}

		return p.node.Reply(msg, resp)
	}

}

func (p *Program) processMsg(m int64) bool {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if contains(p.messages, m) {
		return false
	}

	p.messages = append(p.messages, m)

	return true
}

func (p *Program) periodicJobs(dur time.Duration, f func()) func() {
	ticker := time.NewTicker(dur)
	return func() {
		for {
			select {
			case <-ticker.C:
				f()
			}
		}
	}
}
