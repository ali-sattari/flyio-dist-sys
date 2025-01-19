package txn

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

type Program struct {
	node   NodeInterface
	linKv  KVInterface
	logger *slog.Logger
}

type workload struct {
	MessageBody
	Txn []Transaction
}

type baseResponse struct {
	Type string `json:"type"`
}
type txnResponse struct {
	Type string        `json:"type"`
	Txn  []Transaction `json:"txn"`
}

var wg sync.WaitGroup

func New(n *MaelstromNodeWrapper, kv *MaelstromKVWrapper, l *slog.Logger) *Program {
	return &Program{
		node:   n,
		linKv:  kv,
		logger: l,
	}
}

func (p *Program) GetHandle(rpc_type string) HandlerFunc {
	return func(msg Message) error {
		var body workload
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var resp any
		switch rpc_type {
		case "init":
			resp = p.handleInit()
		case "txn":
			resp = p.handleTxn(body)
		case "txn_ok":

		}

		return p.node.Reply(msg, resp)
	}
}

func (p *Program) handleInit() baseResponse {
	for _, n := range p.node.NodeIDs() {
		if n != p.node.ID() {
			// p.topo = append(p.topo, n)
		}
	}
	// log.Printf("handleInit: topology for %s updated to: %+v", p.node.ID(), p.topo)
	return baseResponse{Type: "init_ok"}
}

func (p *Program) handleTxn(body workload) txnResponse {
	resp := txnResponse{}
	return resp
}

func (p *Program) Shutdown() {
	// close(p.storageChan)
	wg.Wait()
}

func (p *Program) periodicJobs(dur time.Duration, f func()) func() {
	ticker := time.NewTicker(dur)
	return func() {
		for {
			<-ticker.C
			f()
		}
	}
}
