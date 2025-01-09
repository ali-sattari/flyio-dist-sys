package kafka

import (
	"log"
	"time"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type gossipLog struct {
	at       int64
	gossipId string
	key      string
	offsets  []int64
}

type receiveGossipResponse struct {
	Type     string `json:"type"`
	GossipId string `json:"gossip_id"`
}

type sendGossipBody struct {
	Type          string  `json:"type"`
	GossipId      string  `json:"gossip_id"`
	Key           string  `json:"key"`
	GossipOffsets []int64 `json:"gossip_offsets"`
}

const bufferAge time.Duration = time.Millisecond * 200

func (p *Program) gossip(src, key string, offset int64) {
	targets := p.topo
	for _, t := range targets {
		if t == src || t == p.node.ID() { // breaking gossip loop
			continue
		}
		// log.Printf("gossip: from %s to %s for %s: %+v", src, t, key, offset)
		p.bufMtx.Lock()
		if _, ok := p.buffer[t]; !ok {
			p.buffer[t] = map[string][]int64{}
		}
		p.buffer[t][key] = append(p.buffer[t][key], offset)
		p.bufMtx.Unlock()
	}
}

func (p *Program) addGossipLog(node, id, key string, offsets []int64) {
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
			key:      key,
			offsets:  offsets,
		},
	)
}

func (p *Program) ackGossip(body workload, msg maelstrom.Message) {
	p.ackMtx.Lock()
	defer p.ackMtx.Unlock()

	for i, l := range p.gossiped[msg.Src] {
		if l.gossipId == body.GossipId {
			// log.Printf("ackGossip: from %s (%s) for %+v\n", msg.Src, body.GossipId, p.gossiped[msg.Src])
			p.gossiped[msg.Src] = remove(p.gossiped[msg.Src], i)
		}
	}
}

func (p *Program) receiveGossip(body workload, msg maelstrom.Message) receiveGossipResponse {
	key := body.Key

	resp := receiveGossipResponse{
		Type:     "gossip_ok",
		GossipId: body.GossipId,
	}

	for _, o := range body.GossipOffsets {
		if p.processMsg(key, o) { // gossip it around if it is unseen
			p.gossip(msg.Src, key, o)
		}
	}
	ks := p.getKeyStore(key)
	log.Printf("receiveGossip: local keystore for %s looks like %+v", key, ks)

	return resp
}

func (p *Program) flushGossipBuffer() {
	p.flushMtx.Lock()
	defer p.flushMtx.Unlock()
	if time.Since(p.lastFlush) >= bufferAge {
		p.bufMtx.Lock()
		defer p.bufMtx.Unlock()
		// log.Printf("flushGossipBuffer: buffer (%d): %+v", len(p.buffer), p.buffer)
		for t, ml := range p.buffer {
			if len(ml) == 0 {
				continue
			}
			for key, offsetList := range ml {
				gid := uuid.NewString()
				p.node.Send(t, sendGossipBody{
					Type:          "gossip",
					GossipId:      gid,
					Key:           key,
					GossipOffsets: offsetList,
				})
				p.addGossipLog(t, gid, key, offsetList)
				delete(p.buffer[t], key)
			}
			p.buffer[t] = make(map[string][]int64)
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
		// log.Printf("resendUnackedGossips: %s: %+v", n, gl)
		for _, l := range gl {
			if time.Since(time.UnixMicro(l.at)) >= time.Second {
				// log.Printf("resendUnackedGossips: resending gossip batch to %s (%s): %+v", n, l.gossipId, l.offsets)
				p.node.Send(n, sendGossipBody{
					Type:          "gossip",
					GossipId:      l.gossipId,
					Key:           l.key,
					GossipOffsets: l.offsets,
				})
			}
		}
	}
	p.ackMtx.Unlock()
}

func (p *Program) processMsg(key string, offset int64) bool {
	ks := p.getKeyStore(key)

	if contains(ks.offsets, offset) {
		return false
	}
	ks.store(offset)
	// log.Printf("processMsg: storing %d for %s through gossip %+v", offset, key, ks)

	return true
}
