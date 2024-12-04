package broadcast

import (
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type sentLog struct {
	at    int64
	msgId int
	msg   int64
}

func (p *Program) broadcast(body workload, msg maelstrom.Message) map[string]any {
	m := body.Message
	resp := map[string]any{
		"type": "broadcast_ok",
	}

	if p.processMsg(m) {
		//fix this not to mix gossip msg body vs broadcast msg body
		p.gossip(msg.Src, []int64{m})
	}

	return resp
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
