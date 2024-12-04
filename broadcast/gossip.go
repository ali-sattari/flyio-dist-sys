package broadcast

import (
	"time"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type gossipLog struct {
	at       int64
	gossipId string
	msgs     []int64
}

const bufferAge time.Duration = time.Millisecond * 100

func (p *Program) gossip(src string, msgs []int64) {
	targets := p.topo[p.node.ID()]

	for _, t := range targets {
		if t == src || t == p.node.ID() { // breaking gossip loop
			continue
		}
		p.bufMtx.Lock()
		p.buffer[t] = append(p.buffer[t], msgs...)
		p.bufMtx.Unlock()
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
			// log.Printf("acked gossip from %s (%s) for %+v\n", src, id, p.gossiped[src])
			p.gossiped[src] = remove(p.gossiped[src], i)
		}
	}
}

func (p *Program) receiveGossip(body workload, msg maelstrom.Message) map[string]any {
	ml := body.MessageList
	resp := map[string]any{
		"gossip_id": body.GossipId,
		"type":      "gossip_ok",
	}

	for _, m := range ml {
		gl := make([]int64, 0)
		if p.processMsg(m) {
			gl = append(gl, m)
		}
		p.gossip(msg.Src, gl)
	}

	return resp
}

func (p *Program) flushGossipBuffer() {
	p.flushMtx.Lock()
	defer p.flushMtx.Unlock()
	if time.Since(p.lastFlush) >= bufferAge {
		p.bufMtx.Lock()
		defer p.bufMtx.Unlock()
		// log.Printf("buffer (%d): %+v", len(p.buffer), p.buffer)
		for t, ml := range p.buffer {
			if len(ml) == 0 {
				continue
			}

			gid := uuid.NewString()
			p.node.Send(t, workload{
				Type:        "gossip",
				GossipId:    gid,
				MessageList: ml,
			})

			p.addGossipLog(t, gid, ml)
			p.buffer[t] = make([]int64, 0)
		}
		p.lastFlush = time.Now()
	}
}

func (p *Program) resendUnackedGossips() {
	p.ackMtx.Lock()
	for n, gl := range p.gossiped {
		if len(gl) == 0 {
			continue
		}
		// log.Printf("resender for %s: %+v", n, gl)
		for _, l := range gl {
			if time.Since(time.UnixMicro(l.at)) >= time.Second {
				// log.Printf("resending gossip batch to %s (%s): %+v", n, l.gossipId, l.msgs)
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
