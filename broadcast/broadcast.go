package broadcast

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	uuid "github.com/google/uuid"
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
	buffer    map[string][]workload
	bufMtx    *sync.Mutex
	lastFlush time.Time
}

type sentLog struct {
	at    int64
	msgId int
	msg   int64
}

type gossipLog struct {
	at       int64
	gossipId string
	msgs     []int64
}

type workload struct {
	maelstrom.MessageBody
	Type        string
	Message     int64
	MessageList []int64 `json:"message_list,omitempty"`
	GossipId    string  `json:"gossip_id,omitempty"`
	Topology    map[string][]string
}

const bufferAge time.Duration = time.Millisecond * 100

func New(n *maelstrom.Node) Program {
	p := Program{
		node:      n,
		mtx:       &sync.RWMutex{},
		ackMtx:    &sync.Mutex{},
		bufMtx:    &sync.Mutex{},
		gossiped:  make(map[string][]gossipLog),
		buffer:    make(map[string][]workload),
		lastFlush: time.Now(),
	}
	p.resendUnackedLog()
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

func (p *Program) broadcast(body workload, msg maelstrom.Message) map[string]any {
	m := body.Message
	resp := map[string]any{
		"type": "broadcast_ok",
	}

	if p.processMsg(m) {
		p.gossip(body, msg)
	}

	return resp
}

func (p *Program) receiveGossip(body workload, msg maelstrom.Message) map[string]any {
	ml := body.MessageList
	resp := map[string]any{
		"gossip_id": body.GossipId,
		"type":      "gossip_ok",
	}

	for _, m := range ml {
		if p.processMsg(m) {
			p.gossip(body, msg)
		}
	}

	return resp
}

func (p *Program) processMsg(m int64) bool {
	if p.haveMessage(m) {
		return false
	}

	p.mtx.Lock()
	p.messages = append(p.messages, m)
	p.mtx.Unlock()

	return true
}

func (p *Program) read(body workload) map[string]any {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	resp := map[string]any{
		"type":     "read_ok",
		"messages": p.messages,
	}
	return resp
}

func (p *Program) topology(body workload) map[string]any {
	p.topo = body.Topology
	resp := map[string]any{
		"type": "topology_ok",
	}
	p.addLongRangeLinks()
	return resp
}

func (p *Program) gossip(body workload, msg maelstrom.Message) {
	targets := p.topo[p.node.ID()]

	for _, t := range targets {
		if t == msg.Src || t == p.node.ID() { // breaking gossip loop
			continue
		}
		p.bufMtx.Lock()
		p.buffer[t] = append(p.buffer[t], body)
		p.bufMtx.Unlock()
	}

	p.flushGossipBuffer()
}

func (p *Program) flushGossipBuffer() {
	if time.Since(p.lastFlush) >= bufferAge {
		p.bufMtx.Lock()
		defer p.bufMtx.Unlock()
		for t, wll := range p.buffer {
			if len(wll) == 0 {
				continue
			}
			ml := make([]int64, len(wll))
			for _, wl := range wll {
				ml = append(ml, wl.Message)
			}

			gid := uuid.NewString()
			p.node.Send(t, workload{
				Type:        "gossip",
				GossipId:    gid,
				MessageList: ml,
			})

			p.addGossipLog(t, gid, ml)
			p.buffer[t] = make([]workload, 0, 0)
		}
		p.lastFlush = time.Now()
	}
}

func (p *Program) addGossipLog(node string, id string, msgs []int64) {
	p.ackMtx.Lock()
	defer p.ackMtx.Unlock()
	if _, ok := p.gossiped[node]; !ok {
		p.gossiped[node] = make([]gossipLog, 0)
	}
	p.gossiped[node] = append(
		p.gossiped[node],
		gossipLog{
			at:       time.Now().UnixMicro(),
			gossipId: id,
			msgs:     msgs,
		},
	)
}

func (p *Program) ackGossip(src string, body workload) {
	p.ackMtx.Lock()
	defer p.ackMtx.Unlock()
	id := body.GossipId
	for i, l := range p.gossiped[src] {
		if l.gossipId == id {
			log.Printf("acked gossip from %s (%s) for %+v\n", src, id, p.gossiped[src])
			p.gossiped[src] = remove(p.gossiped[src], i)
		}
	}
}

func (p *Program) haveMessage(msg int64) bool {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	for _, m := range p.messages {
		if m == msg {
			return true
		}
	}
	return false
}

func (p *Program) addSentLog(node string, id int, msg int64) {
	p.ackMtx.Lock()
	defer p.ackMtx.Unlock()
	if _, ok := p.sent[node]; !ok {
		p.sent[node] = make([]sentLog, 0)
	}
	p.sent[node] = append(
		p.sent[node],
		sentLog{
			at:    time.Now().UnixMicro(),
			msgId: id,
			msg:   msg,
		},
	)
}

func (p *Program) ackSentLog(src string, body workload) {
	p.ackMtx.Lock()
	defer p.ackMtx.Unlock()
	id := body.InReplyTo
	for i, l := range p.sent[src] {
		if l.msgId == id {
			p.sent[src] = remove(p.sent[src], i)
		}
	}
}

func remove[T any](slice []T, i int) []T {
	if i > len(slice) {
		return slice
	}
	if i == len(slice) {
		return slice[:len(slice)-1]
	}
	slice[i] = slice[len(slice)-1]
	return slice[:len(slice)-1]
}

func (p *Program) resendUnackedLog() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case tc := <-ticker.C:
				p.ackMtx.Lock()
				log.Printf("ticker: %+v", tc)
				for n, gl := range p.gossiped {
					if len(gl) == 0 {
						continue
					}
					log.Printf("resender for %s: %+v", n, gl)
					for _, l := range gl {
						if time.Since(time.UnixMicro(l.at)) >= time.Second {
							log.Printf("resending gossip batch to %s (%s): %+v", n, l.gossipId, l.msgs)
							p.node.Send(n, workload{
								Type:        "gossip",
								GossipId:    l.gossipId,
								MessageList: l.msgs,
							})
						}
					}
				}
				p.ackMtx.Unlock()
			}
		}
	}()
}

func (p *Program) addLongRangeLinks() {
	nodes := p.node.NodeIDs()
	numLinks := int(len(nodes) / 3)

	// Add long-range links
	nc := len(nodes)
	for i := 0; i < numLinks; i++ {
		for {
			// Select two distinct nodes from left and right
			l, r := nodes[i], nodes[nc-i-1]
			if l != r && !contains(p.topo[l], r) {
				// Add bidirectional link
				p.topo[l] = append(p.topo[l], r)
				p.topo[r] = append(p.topo[r], l)
				break
			}
		}
	}
	log.Printf("added long range links to topology: %+v", p.topo)
}

func contains(slice []string, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}
