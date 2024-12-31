package broadcast

import (
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
		p.gossip(msg.Src, []int64{m})
	}

	return resp
}
