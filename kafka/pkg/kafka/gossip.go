package kafka

import (
	"time"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type gossipLog struct {
	at       int64
	gossipId string
	key      string
	messages []MsgLog
}

type receiveGossipResponse struct {
	Type     string `json:"type"`
	GossipId string `json:"gossip_id"`
}

type sendGossipBody struct {
	Type           string   `json:"type"`
	GossipId       string   `json:"gossip_id"`
	Key            string   `json:"key"`
	GossipMessages []MsgLog `json:"gossip_messages"`
}

const bufferAge time.Duration = time.Millisecond * 200

func (p *Program) gossip(src, key string, msg MsgLog) {
	targets := p.topo
	for _, t := range targets {
		if t == src || t == p.node.ID() { // breaking gossip loop
			continue
		}
		// log.Printf("gossip: from %s to %s for %s: %+v", src, t, key, msg)
		p.bufMtx.Lock()
		if _, ok := p.buffer[t]; !ok {
			p.buffer[t] = map[string][]MsgLog{}
		}
		p.buffer[t][key] = append(p.buffer[t][key], msg)
		p.bufMtx.Unlock()
	}
}

func (p *Program) addGossipLog(node, id, key string, messages []MsgLog) {
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
			messages: messages,
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

	for _, m := range body.GossipMessages {
		if p.processMsg(key, m) { // gossip it around if it is unseen
			p.gossip(msg.Src, key, m)
		}
	}
	// log.Printf("receiveGossip: local keystore for %s looks like %+v", key, p.getKeyStore(key))

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
			for key, msgList := range ml {
				gid := uuid.NewString()
				p.node.Send(t, sendGossipBody{
					Type:           "gossip",
					GossipId:       gid,
					Key:            key,
					GossipMessages: msgList,
				})
				p.addGossipLog(t, gid, key, msgList)
				delete(p.buffer[t], key)
			}
			p.buffer[t] = make(map[string][]MsgLog)
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
					Type:           "gossip",
					GossipId:       l.gossipId,
					Key:            l.key,
					GossipMessages: l.messages,
				})
			}
		}
	}
	p.ackMtx.Unlock()
}

func (p *Program) processMsg(key string, msg MsgLog) bool {
	ks := p.getKeyStore(key)

	if contains(ks.offsets, msg.Offset) {
		return false
	}
	ks.store(msg)
	// log.Printf("processMsg: storing %d for %s through gossip %+v", offset, key, ks)

	return true
}
